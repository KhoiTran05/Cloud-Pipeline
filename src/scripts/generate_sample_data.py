import pandas as pd
import numpy as np
from datetime import datetime, timedelta,date
import random
import os

np.random.seed(42)
random.seed(42)

os.makedirs('data/raw', exist_ok=True)

start_date = datetime(2024, 7, 1)
end_date = datetime(2025, 11, 1)

first_names = [
    "Lan", "Mai", "Ngọc", "Hương", "Thảo", 
    "Linh", "Phương", "Trang", "Yến", "Hà", 
    "Anh", "Hiền", "Thu", "Diệu", "Kiều",
    "Bảo", "Đức", "Hoàng", "Khánh", "Long",
    "Minh", "Nam", "Quang", "Sơn", "Thắng",
    "Tài", "Trung", "Tùng", "Vinh", "Duy"
]
last_names = [
    "Nguyễn", "Trần", "Lê", "Phạm", "Huỳnh", 
    "Hoàng", "Võ", "Vũ", "Phan", "Trương", 
    "Bùi", "Đặng", "Đỗ", "Ngô", "Dương"
]
cities = [
    "Hà Nội", "Thành phố Hồ Chí Minh", "Đà Nẵng", 
    "Hải Phòng", "Cần Thơ", "Huế", 
    "Nha Trang", "Đà Lạt", "Hạ Long", "Vinh"
]
age_groups = ['18-25', '26-35', '36-45', '46-55', '56-65', '65+']

# 1.Generate customers df
from itertools import chain

phone_prefixes = ['09', '03', '07', '08', '05']

n_customers = 1000
customer_list =[]
for customer_id in range(1, n_customers + 1):
    email = f"CUST{customer_id}@gmail.com"
    phone_number = random.choice(phone_prefixes)
    for _ in range(8):
        phone_number += str(random.randint(0, 9))
        
    customer_list.append({
        'customer_id': customer_id,
        'first_name': random.choice(first_names), 
        'last_name': random.choice(last_names),
        'email': email,
        'phone_number': phone_number,
        'city': random.choices(population=cities, weights=[50, 10, 5, 5, 5, 5, 5, 5, 5, 5], k=1)[0],
        'age_group': random.choice(age_groups)
    })

customers_df = pd.DataFrame(customer_list)
customers_df.to_csv('data/raw/customers.csv', index=False)

# 2.Generate orders_detail df
n_orders = 10000

product_option_price = {
    range(1, 6): 339286,
    range(6, 9): 565098,
    range(9, 13): 207660,
    tuple(chain(range(13, 19), range(23, 27))): 373029,
    range(19, 22): 251461,
    (22, ): 46000,
    range(27, 33): 303091
}
product_option_product_id = {
    range(1, 6): 1,
    range(6 ,9): 2,
    range(9, 13): 3,
    range(13, 16): 4,
    range(16, 19): 5,
    range(19, 22): 6,
    (22, ): 7,
    range(23, 25): 8,
    range(25, 27): 9,
    range(27, 33): 10
}

order_detail_list = []
order_detail_id_counter = 1

for order_id in range(1, n_orders+1):
    # Each order has 1 -> 4 products
    for _ in range(random.randint(1, 4)):
        product_option_id = random.randint(1, 32)
        quantity = random.randint(1, 3)
        unit_price_list = [v for k,v in product_option_price.items() if product_option_id in k]
        unit_price = unit_price_list[0] if unit_price_list else -1
        product_id_list = [v for k, v in product_option_product_id.items() if product_option_id in k]
        product_id = product_id_list[0] if product_id_list else -1
        
        order_detail = {
            'id': order_detail_id_counter,
            'order_id': order_id,
            'product_id': product_id,
            'product_option_id': product_option_id,
            'quantity': quantity,
            'unit_price': unit_price,
            'total_price': round(quantity*unit_price, 2)
        }
        order_detail_list.append(order_detail)
        order_detail_id_counter += 1
        
orders_detail_df = pd.DataFrame(order_detail_list)
orders_detail_df.to_csv('data/raw/orders_detail.csv', index=False)

# 3.Generate orders df
order_total_amount_dict = orders_detail_df \
    .groupby('order_id')['total_price'] \
    .sum().round(2) \
    .to_dict()

orders_list = []
for order_id in range(1, n_orders+1):
    order_date = start_date + timedelta(days=random.randint(0, (end_date-start_date).days))
    due_date = order_date + timedelta(days=random.randint(5, 10))
    status = random.choices(population=['pending', 'confirmed', 'shipping', 'delivered', 'cancelled'], weights=[5, 25, 25, 40, 5], k=1)[0]
    ship_date = (order_date + timedelta(days=random.randint(1, 3))).date() if status in ['shipping', 'delivered'] else date(9999, 12, 31)
    total_amount = order_total_amount_dict[order_id]
    shipping_fee = random.randint(10, 50)*1000
    discount_amount = random.randint(10, 100)*1000
    final_amount = round(total_amount + shipping_fee - discount_amount, 2)
    payment_method = 'cod'
    payment_status = random.choices(population=['pending', 'paid', 'failed', 'refunded'], weights=[10, 60, 10, 20], k=1)[0]
    
    orders_list.append({
        'id': order_id,
        'customer_id': random.randint(1, 1000),
        'order_date': order_date,
        'due_date': due_date,
        'ship_date': ship_date,
        'status': status,
        'total_amount': total_amount,
        'shipping_fee': shipping_fee,
        'discount_amount': discount_amount,
        'final_amount': final_amount,
        'payment_method': payment_method,
        'payment_status': payment_status
    })
    
orders_df = pd.DataFrame(orders_list)
orders_df.to_csv('data/raw/orders.csv', index=False)