# Master Context

## Project Purpose
Automated E-Commerce Sales & Inventory Analytics Pipeline built on AWS with Python, PySpark, Athena, and Power BI.

## Core Stack
- **Languages**: Python 3.11+, PySpark 3.5+, SQL (Athena), DAX
- **Cloud Infrastructure**: AWS S3, AWS Glue, AWS Lambda, IAM, Glue Data Catalog, Athena
- **BI / Visualization**: Power BI Desktop, Simba Athena ODBC Driver
- **Testing & CI/CD**: pytest, chispa, GitHub Actions

## Repository Layout
- `docs/`: PRD, TRD, architecture diagram, project plan, and key decisions.
- `coordination/`: Cross-agent and developer coordination logs, task boards, and contracts.
- `data/`: Mock generator and raw staging data.
- `src/`: PySpark ETL transformation logic (Silver and Gold) and utilities.
- `tests/`: Chispa DataFrame tests and unit tests.
- `aws/`: Infrastructure scripts, IAM policies, Athena DDL, and Lambda trigger.
- `dax/`: Power BI DAX calculation models.
- `.github/`: GitHub Actions automation workflows.
