// File: index.mjs
// Runtime: Node.js 20.x

import { BigQuery } from '@google-cloud/bigquery';
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import mysql from 'mysql2/promise';

// --- Hàm phụ (Helpers) ---

/**
 * Lấy "chìa khóa" JSON của Google Service Account từ AWS Secrets Manager.
 */
async function getGCPCredentials() {
    const secretId = "gcp/bigquery-writer-key"; 
    const client = new SecretsManagerClient();
    const command = new GetSecretValueCommand({ SecretId: secretId });
    try {
        const response = await client.send(command);
        return response.SecretString;
    } catch (error) {
        console.error("Lỗi khi lấy Secret từ AWS Secrets Manager:", error);
        throw new Error("Không thể lấy GCP credentials.");
    }
}

/**
 * Khởi tạo kết nối RDS (MySQL)
 */
async function getRDSConnection() {
    return await mysql.createConnection({
        host: process.env.RDS_HOST,
        user: process.env.RDS_USER,
        password: process.env.RDS_PASSWORD,
        database: process.env.RDS_DB,
        port: 3306
    });
}

// --- Hàm chính (Main Handler) ---

export const handler = async (event) => {
    console.log("Bắt đầu đồng bộ RDS (MySQL) -> BigQuery...");

    let rdsConnection; 
    
    try {
        // 1. Lấy "chìa khóa" GCP và khởi tạo BigQuery
        const secretString = await getGCPCredentials();
        const credentials = JSON.parse(secretString);
        const bigquery = new BigQuery({
            projectId: credentials.project_id,
            credentials
        });
        const bqDataset = bigquery.dataset("ecommerce_analytics"); // Tên Dataset 

        // 2. Kết nối AWS RDS
        rdsConnection = await getRDSConnection();
        console.log("Đã kết nối thành công AWS RDS (MySQL).");

        // 3. Định nghĩa các câu truy vấn (Queries)
        const orders_query = `SELECT id AS order_id, customer_id, order_date, due_date, ship_date, status, total_amount, shipping_fee, discount_amount, final_amount, address, currency, payment_method, payment_status, customer_note, admin_note FROM orders WHERE updated_at > (NOW() - INTERVAL 1 HOUR)`;
        const customers_query = `SELECT customer_id, first_name, last_name, email, phone_number, city, age_group FROM customers WHERE updated_at > (NOW() - INTERVAL 1 HOUR)`;
        const orders_detail_query = `SELECT id AS orders_detail_id, order_id, product_id, product_option_id, quantity, unit_price, total_price FROM orders_detail WHERE updated_at > (NOW() - INTERVAL 1 HOUR)`;

        // 4. Extract (Đọc): Thực thi CẢ 3 truy vấn song song (parallel)
        console.log("Đang thực thi 3 truy vấn song song...");
        
        const [
            [orders_rows],    // Kết quả của orders_query
            [customers_rows], // Kết quả của customers_query
            [details_rows]    // Kết quả của orders_detail_query
        ] = await Promise.all([
            rdsConnection.execute(orders_query),
            rdsConnection.execute(customers_query),
            rdsConnection.execute(orders_detail_query)
        ]);

        console.log(`Tìm thấy: ${orders_rows.length} đơn hàng; ${customers_rows.length} khách hàng; ${details_rows.length} chi tiết.`);

        // 5. Load (Ghi): Ghi dữ liệu vào BigQuery (cũng chạy song song)
        
        const insert_promises = [];

        // Chèn Orders (nếu có)
        if (orders_rows.length > 0) {
            insert_promises.push(
                bqDataset.table("orders").insert(orders_rows) // Tên Bảng 
            );
        }
        
        // Chèn Customers (nếu có)
        if (customers_rows.length > 0) {
            insert_promises.push(
                bqDataset.table("customers").insert(customers_rows) // Bảng mới
            );
        }

        // Chèn Order Details (nếu có)
        if (details_rows.length > 0) {
            insert_promises.push(
                bqDataset.table("orders_detail").insert(details_rows) // Bảng mới
            );
        }

        if (insert_promises.length === 0) {
            console.log("Không có dữ liệu mới nào để đồng bộ.");
            return { statusCode: 200, body: "Không có dữ liệu mới." };
        }

        // Chờ tất cả các tác vụ chèn hoàn tất
        await Promise.all(insert_promises);

        const summary = `Đồng bộ thành công: ${orders_rows.length} đơn hàng, ${customers_rows.length} khách hàng, ${details_rows.length} chi tiết.`;
        console.log(summary);
        return { statusCode: 200, body: summary };

    } catch (err) {
        console.error("LỖI ETL:", err);
        return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
    } finally {
        if (rdsConnection) {
            await rdsConnection.end();
            console.log("Đã đóng kết nối RDS.");
        }
    }
};