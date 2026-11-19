# 2-Day Project Execution Plan

See the full architectural breakdown in [ecommerce_2day_project_plan.md](file:///C:/Users/gargk/.gemini/antigravity/brain/368afce0-05e2-468d-bede-da3e2be443a7/ecommerce_2day_project_plan.md).

## Day 1: Local Foundation & Medallion ETL
- **Phase 1**: Local Environment, Scaffolding & Faker Mock Generator (orders, inventory, customers)
- **Phase 2**: PySpark Silver Cleaning & Gold KPI Aggregations with pytest/chispa tests
- **Phase 3**: AWS S3 Storage Hierarchy & Least-Privilege IAM Policies

## Day 2: Cloud Ingestion, CI/CD & Power BI
- **Phase 4**: S3 Event Notification -> AWS Lambda Trigger -> AWS Glue PySpark Execution & GitHub Actions CI/CD
- **Phase 5**: Athena Glue Data Catalog & Power BI Dashboards (Sales, Inventory Health, CLV)
- **Phase 6**: Data Reconciliation, Failover Testing & Project Sign-Off
