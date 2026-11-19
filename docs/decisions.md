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
