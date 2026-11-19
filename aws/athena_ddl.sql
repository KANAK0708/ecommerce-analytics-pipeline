-- ==========================================================
-- Amazon Athena DDL for Ecommerce Analytics Gold Datasets
-- Replace <YOUR_S3_BUCKET_NAME> with your actual bucket name
-- ==========================================================

CREATE DATABASE IF NOT EXISTS ecommerce_analytics_db
COMMENT 'Medallion Gold Aggregated Analytics Datasets'
LOCATION 's3://<YOUR_S3_BUCKET_NAME>/gold/';

-- 1. Gold Daily Sales Table
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_analytics_db.gold_daily_sales (
    order_date DATE,
    total_revenue DOUBLE,
    total_orders BIGINT,
    aov DOUBLE
)
STORED AS PARQUET
LOCATION 's3://<YOUR_S3_BUCKET_NAME>/gold/gold_daily_sales/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- 2. Gold Inventory Health Table
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_analytics_db.gold_inventory_health (
    product_id STRING,
    product_name STRING,
    category STRING,
    current_stock INT,
    reorder_threshold INT,
    reorder_flag INT
)
STORED AS PARQUET
LOCATION 's3://<YOUR_S3_BUCKET_NAME>/gold/gold_inventory_health/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');

-- 3. Gold Customer Metrics Table
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_analytics_db.gold_customer_metrics (
    customer_id STRING,
    total_spent DOUBLE,
    total_orders BIGINT,
    clv_tier STRING
)
STORED AS PARQUET
LOCATION 's3://<YOUR_S3_BUCKET_NAME>/gold/gold_customer_metrics/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
