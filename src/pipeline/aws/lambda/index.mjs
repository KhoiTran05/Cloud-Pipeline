// File: index.mjs
// Runtime: Node.js 20.x

import { GoogleAuth } from 'google-auth-library';
import { BigQuery } from '@google-cloud/bigquery';
import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import mysql from 'mysql2/promise';
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb";

const DATASET_ID = "hnv_staging";
const CHUNK_SIZE = 500; 

const WATERMARK_TABLE_NAME = "DataPipelineWatermarks"; 
const PIPELINE_IDS = {
    ORDERS: 'rds_orders',
    CUSTOMERS: 'rds_customers',
    DETAILS: 'rds_orders_detail',
    CATEGORIES: 'rds_categories',
    PRODUCTS: 'rds_products',
    PRODUCT_OPTIONS : 'rds_product_options'
};
const MIN_TIMESTAMP = '1970-01-01 00:00:00'; 

// --- Helper Functions ---

function chunkArray(array, chunkSize) {
    const chunks = [];
    for (let i = 0; i < array.length; i += chunkSize) {
        chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
}

function normalizeWM(wm) {
    if (!wm) return "1970-01-01 00:00:00";
    if (wm instanceof Date)
      return wm.toISOString().slice(0,19).replace("T"," ");
    return String(wm).replace("T"," ").slice(0,19);
}

async function getGCPCredentials() {
    const secretId = "gcp/bigquery-writer-key";
    const client = new SecretsManagerClient();
    const command = new GetSecretValueCommand({ SecretId: secretId });
    try {
        const response = await client.send(command);
        return response.SecretString;
    } catch (error) {
        console.error("Lỗi AWS Secrets:", error);
        throw new Error("Không thể lấy GCP credentials.");
    }
}

// --- DynamoDB Helpers ---
async function getWatermark(docClient, pipelineId) {
    const command = new GetCommand({
        TableName: WATERMARK_TABLE_NAME,
        Key: { pipeline_id: pipelineId }
    });
    try {
        const response = await docClient.send(command);
        return response.Item?.last_successful_timestamp || MIN_TIMESTAMP;
    } catch (e) {
        console.error(`Lỗi đọc watermark ${pipelineId}:`, e);
        return MIN_TIMESTAMP;
    }
}

async function updateWatermark(docClient, pipelineId, newTimestamp) {
    if (!newTimestamp) return; 
    const command = new PutCommand({
        TableName: WATERMARK_TABLE_NAME,
        Item: {
            pipeline_id: pipelineId,
            last_successful_timestamp: newTimestamp
        }
    });
    await docClient.send(command);
}

function findNewMaxTimestamp(rows, oldWatermark) {
    if (!rows || !Array.isArray(rows) || rows.length === 0) {
        return oldWatermark;
    }
    const lastRow = rows[rows.length - 1];
    let newMax = lastRow.updated_at;
    if (newMax instanceof Date) {
        newMax = newMax.toISOString().slice(0, 19).replace('T', ' ');
    }
    return newMax || oldWatermark;
}

async function processTable(bqDataset, tableName, rows) {
    if (!rows || !Array.isArray(rows) || rows.length === 0) return 0;

    const sanitized = rows.map(r => {
        const obj = {};
        for (const k in r) {
            let v = r[k];
            if (v instanceof Date) {
                 v = v.toISOString().slice(0, 19).replace('T', ' ');
            }
            if (v === null) v = undefined; 
            obj[k] = v;
        }
        return obj;
    });

    const chunks = chunkArray(sanitized, CHUNK_SIZE);
    for (const chunk of chunks) {
        await bqDataset.table(tableName).insert(chunk);
    }
    return rows.length;
}

// --- MAIN HANDLER ---

export const handler = async (event) => {
    console.log("--- BẮT ĐẦU ETL JOB (POOL MODE) ---");
    
    const ddbClient = new DynamoDBClient({});
    const docClient = DynamoDBDocumentClient.from(ddbClient);
    let newWatermarks = {};

    const pool = mysql.createPool({
        host: process.env.RDS_HOST,
        user: process.env.RDS_USER,
        password: process.env.RDS_PASSWORD,
        database: process.env.RDS_DB,
        port: 3306,
        waitForConnections: true,
        connectionLimit: 10,
        queueLimit: 0,
        timezone: '+00:00', 
        enableKeepAlive: true,
        keepAliveInitialDelay: 0
    });

    async function safeQuery(sql, params) {
        try {
          const [rows] = await pool.execute(sql, params);
          return rows || [];
        } catch (err) {
          console.error("Query failed:", sql, err);
          return []; // tránh undefined làm lỗi length
        }
    }

    try {
        // 1. Setup GCP Credentials
        const secretString = await getGCPCredentials();
        const credentials = JSON.parse(secretString);
        
        const bigquery = new BigQuery({
            projectId: credentials.project_id,
            credentials
        });
        const bqDataset = bigquery.dataset(DATASET_ID);
        
        // 2. Lấy Watermarks
        const watermarks = await Promise.all([
            getWatermark(docClient, PIPELINE_IDS.ORDERS),
            getWatermark(docClient, PIPELINE_IDS.CUSTOMERS),
            getWatermark(docClient, PIPELINE_IDS.DETAILS),
            getWatermark(docClient, PIPELINE_IDS.CATEGORIES),
            getWatermark(docClient, PIPELINE_IDS.PRODUCTS),
            getWatermark(docClient, PIPELINE_IDS.PRODUCT_OPTIONS)
        ]);
        
        const wmOrders = normalizeWM(watermarks[0]);
        const wmCustomers = normalizeWM(watermarks[1]);
        const wmDetails = normalizeWM(watermarks[2]);
        const wmCategories = normalizeWM(watermarks[3]);
        const wmProducts = normalizeWM(watermarks[4]);
        const wmOptions = normalizeWM(watermarks[5]);
        console.log("Watermarks hiện tại:", JSON.stringify(watermarks));

        // 3. Execute Queries (SỬ DỤNG POOL)
        const [
            rowsOrders,
            rowsCustomers,
            rowsDetails,
            rowsCategories,
            rowsProducts,
            rowsOptions
        ] = await Promise.all([
            safeQuery(`SELECT * FROM orders WHERE updated_at > ? ORDER BY updated_at`, [wmOrders]),
            safeQuery(`SELECT * FROM customers WHERE updated_at > ? ORDER BY updated_at`, [wmCustomers]),
            safeQuery(`SELECT * FROM orders_detail WHERE updated_at > ? ORDER BY updated_at`, [wmDetails]),
            safeQuery(`SELECT * FROM categories WHERE updated_at > ? ORDER BY updated_at`, [wmCategories]),
            safeQuery(`SELECT * FROM products WHERE updated_at > ? ORDER BY updated_at`, [wmProducts]),
            safeQuery(`SELECT * FROM product_options WHERE updated_at > ? ORDER BY updated_at`, [wmOptions])
        ]);
        
        console.log(
            `Extract: Orders(${rowsOrders.length}), Customers(${rowsCustomers.length}), Details(${rowsDetails.length}), Categories(${rowsCategories.length}, Products(${rowsProducts.length}), ProductOptions(${rowsOptions.length}))`
        );

        // 4. Tính toán Watermark Mới
        newWatermarks.orders = findNewMaxTimestamp(rowsOrders, wmOrders);
        newWatermarks.customers = findNewMaxTimestamp(rowsCustomers, wmCustomers);
        newWatermarks.details = findNewMaxTimestamp(rowsDetails, wmDetails);
        newWatermarks.categories = findNewMaxTimestamp(rowsCategories, wmCategories);
        newWatermarks.products = findNewMaxTimestamp(rowsProducts, wmProducts);
        newWatermarks.productOptions = findNewMaxTimestamp(rowsOptions, wmOptions);

        // 5. Load to BigQuery
        const loadResults = await Promise.all([
            processTable(bqDataset, "orders_temp", rowsOrders),
            processTable(bqDataset, "customers_temp", rowsCustomers),
            processTable(bqDataset, "orders_detail_temp", rowsDetails),
            processTable(bqDataset, "categories_temp", rowsCategories),
            processTable(bqDataset, "products_temp", rowsProducts),
            processTable(bqDataset, "product_options_temp", rowsOptions)
        ]);
        console.log(`Load to BigQuery: ${loadResults.join(", ")}`);
        console.log("Load to BigQuery thành công.");

        const totalSynced = loadResults.reduce((a, b) => a + b, 0);

        // 6. Kích hoạt Cloud Workflow
        if (totalSynced > 0) {
            console.log(`Đã tải ${totalSynced} dòng. Kích hoạt Workflow...`);
            console.log(credentials.client_email);

            const auth = new GoogleAuth({
                credentials,
                scopes: ['https://www.googleapis.com/auth/cloud-platform']
            });

            const client = await auth.getClient();
            const tokenResponse = await client.getAccessToken(); 
            const accessToken = tokenResponse.token || tokenResponse; 

            const workflowUrl = `https://workflowexecutions.googleapis.com/v1/projects/${credentials.project_id}/locations/asia-southeast1/workflows/etl-pipeline-workflow/executions`;
            const executionArgs = { projectId: credentials.project_id };

            const triggerRes = await fetch(workflowUrl, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${accessToken}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ argument: JSON.stringify(executionArgs) }) 
            });

            if (!triggerRes.ok) {
                const errText = await triggerRes.text();
                throw new Error(`Workflow Trigger Failed: ${triggerRes.status} ${errText}`);
            }
            console.log("Workflow kích hoạt thành công.");
        } else {
            console.log("Không có dữ liệu mới. Bỏ qua Workflow.");
        }

        // 7. Cập nhật Watermark
        console.log("Cập nhật Watermarks vào DynamoDB...");
        await Promise.all([
            updateWatermark(docClient, PIPELINE_IDS.ORDERS, newWatermarks.orders),
            updateWatermark(docClient, PIPELINE_IDS.CUSTOMERS, newWatermarks.customers),
            updateWatermark(docClient, PIPELINE_IDS.DETAILS, newWatermarks.details),
            updateWatermark(docClient, PIPELINE_IDS.CATEGORIES, newWatermarks.categories),
            updateWatermark(docClient, PIPELINE_IDS.PRODUCTS, newWatermarks.products),
            updateWatermark(docClient, PIPELINE_IDS.PRODUCT_OPTIONS, newWatermarks.productOptions)
        ]);

        return { statusCode: 200, body: "ETL Success" };

    } catch (err) {
        console.error("CRITICAL ERROR:", err);
        return { statusCode: 500, body: err.message };
    } 
 
};