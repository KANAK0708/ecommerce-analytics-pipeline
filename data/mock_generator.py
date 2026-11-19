"""
Mock Data Generator for Automated E-Commerce Sales & Inventory Analytics Pipeline.
Generates realistic e-commerce datasets with deliberate edge cases for testing data quality rules.

Datasets generated:
1. orders.csv (order_id, customer_id, order_date, product_id, quantity, unit_price)
2. inventory.csv (product_id, product_name, category, current_stock, reorder_threshold)
3. customers.csv (customer_id, signup_date, country)
"""

import os
import random
import csv
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "raw")

CATEGORIES = [
    "Electronics", "Home & Kitchen", "Apparel", 
    "Health & Personal Care", "Books", "Sports & Outdoors"
]

COUNTRIES = ["US", "CA", "GB", "DE", "FR", "IN", "JP", "AU"]

def generate_customers(num_customers=1000):
    """Generate customer master dataset with edge cases."""
    customers = []
    customer_ids = [f"CUST_{10000 + i}" for i in range(num_customers)]
    
    for cust_id in customer_ids:
        # Realistic signup date between 2 years ago and today
        days_ago = random.randint(1, 730)
        signup_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        country = random.choice(COUNTRIES)
        
        # Edge Case 1: Injected null country (1% probability)
        if random.random() < 0.01:
            country = ""
            
        # Edge Case 2: Injected future signup date (0.5% probability)
        if random.random() < 0.005:
            signup_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
            
        customers.append({
            "customer_id": cust_id,
            "signup_date": signup_date,
            "country": country
        })
    
    # Edge Case 3: Duplicate customer rows (simulate dirty source feeds)
    duplicates = random.sample(customers, k=int(num_customers * 0.02))
    customers.extend(duplicates)
    
    return customers, customer_ids

def generate_inventory(num_products=300):
    """Generate product inventory dataset with edge cases."""
    inventory = []
    product_ids = [f"PROD_{1000 + i}" for i in range(num_products)]
    
    for prod_id in product_ids:
        category = random.choice(CATEGORIES)
        prod_name = f"{fake.word().capitalize()} {random.choice(['Pro', 'Max', 'Plus', 'Mini', 'Classic', 'Lite'])}"
        threshold = random.choice([10, 20, 25, 50, 75, 100])
        # Mix of healthy stock, low stock (below threshold), and out-of-stock
        stock = random.choice([
            random.randint(0, 5),               # Critical / stockout risk
            random.randint(5, threshold),       # Reorder triggered
            random.randint(threshold + 1, 500)  # Healthy stock
        ])
        
        # Edge Case 1: Negative stock anomaly (e.g. ERP desync, 1% probability)
        if random.random() < 0.01:
            stock = -1 * random.randint(1, 10)
            
        # Edge Case 2: Missing product name (0.5% probability)
        if random.random() < 0.005:
            prod_name = ""
            
        inventory.append({
            "product_id": prod_id,
            "product_name": prod_name,
            "category": category,
            "current_stock": stock,
            "reorder_threshold": threshold
        })
        
    return inventory, product_ids

def generate_orders(customer_ids, product_ids, num_orders=15000):
    """Generate transactional orders dataset with edge cases."""
    orders = []
    base_date = datetime.now() - timedelta(days=180)
    
    # Price dictionary for consistency
    product_prices = {
        prod_id: round(random.uniform(5.99, 499.99), 2)
        for prod_id in product_ids
    }
    
    for i in range(num_orders):
        order_id = f"ORD_{100000 + i}"
        customer_id = random.choice(customer_ids)
        prod_id = random.choice(product_ids)
        quantity = random.choices([1, 2, 3, 4, 5, 10], weights=[0.5, 0.25, 0.12, 0.06, 0.05, 0.02])[0]
        unit_price = product_prices[prod_id]
        
        # Spread dates over past 6 months
        order_date = (base_date + timedelta(days=random.randint(0, 180), minutes=random.randint(0, 1440))).strftime("%Y-%m-%d")
        
        # Edge Case 1: Injected null customer_id (1% probability)
        if random.random() < 0.01:
            customer_id = ""
            
        # Edge Case 2: Negative quantity (return/cancellation artifact without proper sign, 0.5% probability)
        if random.random() < 0.005:
            quantity = -1 * quantity
            
        # Edge Case 3: Zero or negative unit price (0.3% probability)
        if random.random() < 0.003:
            unit_price = 0.0
            
        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date,
            "product_id": prod_id,
            "quantity": quantity,
            "unit_price": unit_price
        })
        
        # Multi-item orders: 30% chance an order has multiple distinct line items
        if random.random() < 0.30:
            second_prod = random.choice(product_ids)
            if second_prod != prod_id:
                orders.append({
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "order_date": order_date,
                    "product_id": second_prod,
                    "quantity": random.randint(1, 3),
                    "unit_price": product_prices[second_prod]
                })

    # Edge Case 4: Duplicate order line rows (1% duplicate noise)
    duplicates = random.sample(orders, k=int(num_orders * 0.01))
    orders.extend(duplicates)
    
    return orders

def save_csv(data, filename, fieldnames):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Generated {len(data):,} records -> {filepath}")

def main():
    print("Generating synthetic datasets with edge cases...")
    customers, customer_ids = generate_customers(num_customers=1200)
    inventory, product_ids = generate_inventory(num_products=350)
    orders = generate_orders(customer_ids, product_ids, num_orders=12000)
    
    save_csv(customers, "customers.csv", ["customer_id", "signup_date", "country"])
    save_csv(inventory, "inventory.csv", ["product_id", "product_name", "category", "current_stock", "reorder_threshold"])
    save_csv(orders, "orders.csv", ["order_id", "customer_id", "order_date", "product_id", "quantity", "unit_price"])
    print("Mock data generation complete!")

if __name__ == "__main__":
    main()
