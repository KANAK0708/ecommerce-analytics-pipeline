# Architectural Decisions & Guardrails

## 1. Storage Architecture (Medallion Pattern)
- **Bronze (`s3://.../bronze/`)**: Preserves raw source CSV/JSON inputs untouched.
- **Silver (`s3://.../silver/`)**: Cleaned, deduplicated, and schema-validated Parquet format (Snappy compression).
- **Gold (`s3://.../gold/`)**: Aggregated business KPIs for direct SQL querying and reporting.

## 2. Anti-Recursion Trigger Policy
- **Decision**: S3 Event Notifications are strictly filtered to the prefix `bronze/` and suffix `.csv`.
- **Reason**: Prevents infinite Lambda execution loops when Glue writes Silver and Gold Parquet outputs back to S3.

## 3. Customer Lifetime Value (CLV) Tiers
- **High Tier**: Total spend $\ge \$1,000$
- **Medium Tier**: $\$300 \le \text{Total spend} < \$1,000$
- **Low Tier**: $\text{Total spend} < \$300$

## 4. Inventory Reorder Flag
- **Rule**: `reorder_flag = 1` if `current_stock <= reorder_threshold`, else `0`.

## 5. Security & IAM
- Dedicated IAM roles for Lambda (`LambdaETLTriggerRole`) and Glue (`GlueETLExecutionRole`) adhering to least privilege. No credentials stored in repository.

## 6. Phase 1 Data Generation & Quality Decisions
- **Approved Dataset Volumes**:
  - Exactly 1,000 unique customer identities (1,010 raw CSV data rows inclusive of 10 duplicate replays).
  - Exactly 300 unique inventory products.
  - Exactly 10,000 order line-item records (inclusive of multi-item orders and 100 duplicate replays).
- **Environment**: Python 3.11 is the approved runtime to maintain local and AWS Glue PySpark 3.5 compatibility.
- **Canonical Product Catalog**: Decoupled pricing interface `get_canonical_product_catalog(seed=42)` enables independent testing of product pricing consistency without altering the TRD schema for `inventory.csv`.
- **Reference Date Parameter**: `reference_date: str` enables calendar-independent, deterministic date generation across environments and test runs.
- **Strict File Deletion Policy**: The `--clean` CLI flag deletes *only* the three confirmed generated output CSV files (`data/raw/customers.csv`, `data/raw/inventory.csv`, `data/raw/orders.csv`). No wildcard deletions are permitted.
- **Orthogonal Anomaly Injection**: Anomaly categories are injected into mutually exclusive rows to eliminate cross-category inflation and ensure deterministic assertion counts.
- **Quality Assurance & Review Roles**: `Codex` performs independent code review and test authoring; `Antigravity` coordinates integration, clean-slate dry-runs, and coordination logging.

## 7. Phase 2 Silver Cleaning & Gold KPI Decisions (approved by Project Owner 2026-11-28)
- **Rejected rows are quarantined, never dropped**: `silver/quarantine/<table>/` keeps the original raw columns plus `rejection_reason` and `rejected_at` (one UTC timestamp per run). One reason per row; the first matching rule wins.
- **Customers**: quarantine blank `country`, future `signup_date` (relative to `reference_date`, default `2026-11-19`, not the wall clock), and duplicate `customer_id` replays.
- **Inventory**: quarantine negative `current_stock` and blank `product_name`.
- **Orders**: quarantine blank `customer_id`, negative `quantity`, zero `unit_price`, and duplicate `(order_id, product_id)` replays. Orders are never rejected because of their relationship with customer signup dates.
- **Schema-conformance checks** (`invalid_<field>`): values that cannot be cast to their contract type, or blank non-nullable keys, are quarantined. They do not fire on the approved Phase 1 data.
- **Deterministic deduplication**: the contract has no timestamp/version column, so the first occurrence in source-file order is kept.
- **Reconciliation** (enforced: raw = silver + quarantine): customers 1,010 = 985 + 25; inventory 300 = 294 + 6; orders 10,000 = 9,700 + 300.
- **Financial precision**: `unit_price` DECIMAL(10,2), line revenue DECIMAL(12,2), aggregates DECIMAL(14,2); Gold money columns are cast to DOUBLE on write to match `aws/athena_ddl.sql`. The Athena DDL is not changed.
- **Revenue** = quantity x unit_price (no tax, discount, shipping or returns). `total_orders` = distinct `order_id`. `aov` = revenue / orders, rounded half-up to 2 decimals.
- **gold_inventory_health** follows the Athena DDL column list (includes `reorder_threshold`).
- **gold_customer_metrics base (Option A)**: only Silver customers (985), left-joined to their orders; zero-order customers are kept with 0.00 spend, 0 orders, tier Low. Known, explained difference: the 15 quarantined customers have 175 valid Silver order lines (132 orders, $88,016.53), which count in `gold_daily_sales` ($5,016,285.01 total) but not in `gold_customer_metrics` ($4,928,268.48 total).
- **Idempotency**: all Silver/Gold outputs are written with overwrite mode; no partitioning at the current data volume.
