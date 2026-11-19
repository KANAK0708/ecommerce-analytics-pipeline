# API & Data Contracts

## 1. Input Datasets (Bronze Layer)
- **`orders.csv`**:
  - `order_id` (STRING, non-null)
  - `customer_id` (STRING, non-null)
  - `order_date` (DATE: `YYYY-MM-DD`)
  - `product_id` (STRING, non-null)
  - `quantity` (INT, $>0$)
  - `unit_price` (DOUBLE, $>0.0$)
- **`inventory.csv`**:
  - `product_id` (STRING, PK)
  - `product_name` (STRING)
  - `category` (STRING)
  - `current_stock` (INT)
  - `reorder_threshold` (INT)
- **`customers.csv`**:
  - `customer_id` (STRING, PK)
  - `signup_date` (DATE: `YYYY-MM-DD`)
  - `country` (STRING)

## 2. Output Datasets (Gold Layer)
- **`gold_daily_sales`**:
  - `order_date` (DATE)
  - `total_revenue` (DOUBLE)
  - `total_orders` (BIGINT)
  - `aov` (DOUBLE)
- **`gold_inventory_health`**:
  - `product_id` (STRING)
  - `product_name` (STRING)
  - `category` (STRING)
  - `current_stock` (INT)
  - `reorder_threshold` (INT)
  - `reorder_flag` (INT: 1 if `current_stock <= reorder_threshold`, else 0)
- **`gold_customer_metrics`**:
  - `customer_id` (STRING)
  - `total_spent` (DOUBLE)
  - `total_orders` (BIGINT)
  - `clv_tier` (STRING: "High", "Medium", "Low")
