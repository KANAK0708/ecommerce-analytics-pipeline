# Automated E-Commerce Sales & Inventory Analytics Pipeline

A production-grade cloud analytics data engineering pipeline adhering to the **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**. Built with **Python 3.11+, PySpark 3.5+, Amazon S3, AWS Glue, AWS Lambda, Amazon Athena, and Power BI**.

---

## Architecture Overview

```
[Faker Mock Generator]
        │ (CSV Upload)
        ▼
[Amazon S3: Bronze Layer] ──(S3 Event: ObjectCreated)──> [AWS Lambda Handler]
                                                                  │
                                                       (start_job_run)
                                                                  ▼
                                                      [AWS Glue PySpark Job]
                                                       ├── Silver Clean
                                                       └── Gold Aggregate
                                                                  │
                 ┌────────────────────────────────────────────────┴────────────────────────┐
                 ▼                                                                         ▼
      [Amazon S3: Silver Parquet]                                               [Amazon S3: Gold Parquet]
      (Cleaned, Deduplicated)                                                   (Aggregated Business KPIs)
                                                                                           │
                                                                                           ▼
                                                                                [AWS Glue Data Catalog]
                                                                                           │
                                                                                           ▼
                                                                                   [Amazon Athena]
                                                                                           │ (Simba ODBC)
                                                                                           ▼
                                                                                 [Power BI Dashboards]
```

---

## 2-Day Sprint Roadmap

* **Day 1**: Local Environment, Scaffolding, Synthetic Data Generator, PySpark Silver/Gold ETL, and Unit Tests.
* **Day 2**: AWS Infrastructure (S3, IAM, Athena DDL), Event-driven Lambda trigger, CI/CD with GitHub Actions, and Power BI Dashboarding with DAX.

See the complete 2-day implementation plan artifact:
[ecommerce_2day_project_plan.md](file:///C:/Users/gargk/.gemini/antigravity/brain/368afce0-05e2-468d-bede-da3e2be443a7/ecommerce_2day_project_plan.md)

---

## Quick Start (Local Run)

### 1. Install Dependencies
```bash
python -m venv venv
# Windows:
.\venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Generate Sample Datasets
```bash
python data/mock_generator.py
```
This will populate `data/raw/` with:
* `orders.csv`
* `inventory.csv`
* `customers.csv`

### 3. Run Unit & DataFrame Tests
```bash
pytest tests/ -v --cov=src
```
