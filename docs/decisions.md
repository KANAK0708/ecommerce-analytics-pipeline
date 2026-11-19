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
