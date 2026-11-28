# Project Task Board

## Backlog (Phase 2)
- [ ] TASK-2.5: Independent Codex review of `feature/phase2-pyspark-etl` (`Codex`)
- [ ] TASK-2.6: Phase 2 human sign-off (`Human`)

## Backlog (Phases 3–5)
- [ ] TASK-3.1: Provision AWS S3 bucket structure and verify IAM permissions
- [ ] TASK-3.2: Create Athena database and tables via `aws/athena_ddl.sql`
- [ ] TASK-4.1: Deploy Lambda trigger with S3 event notification
- [ ] TASK-4.2: Configure GitHub Actions secrets and test deployment workflow
- [ ] TASK-5.1: Configure Power BI Simba ODBC connection
- [ ] TASK-5.2: Build Power BI dashboards (Sales, Inventory, CLV)
- [ ] TASK-5.3: Reconcile Power BI numbers with Athena SQL query output

## In Progress
*(None. Phase 2 cleaning rules approved by Project Owner 2026-11-28; implementation pushed for review.)*

## Done (Phase 2: PySpark Medallion ETL — awaiting review)
- [x] TASK-2.0: Local Spark session utility (`src/utils/spark_session.py`) (`Claude Code`) — *Completed 2026-11-28*
- [x] TASK-2.1: PySpark Silver cleaning with quarantine (`src/etl/silver_clean.py`) (`Claude Code`) — *Completed 2026-11-28*
- [x] TASK-2.2: PySpark Gold KPI aggregations (`src/etl/gold_aggregate.py`) (`Claude Code`) — *Completed 2026-11-28*
- [x] TASK-2.3: Chispa/pytest tests for Spark, Silver, Gold and pipeline (`tests/`) (`Claude Code`) — *Completed 2026-11-28*
- [x] TASK-2.4: Local end-to-end runner (`src/pipeline.py`) (`Claude Code`) — *Completed 2026-11-28*

## Done (Phase 1: Local Development & Synthetic Data Engine)
- [x] TASK-1.0: Local Python 3.11 Virtual Environment & Dependency Setup (`Human`) — *Completed 2026-11-23*
- [x] TASK-1.1: Formalize Phase 1 Data Contracts & Anomaly Matrix (`Antigravity`) — *Completed 2026-11-19*
- [x] TASK-1.2: Standardize `data/mock_generator.py` Engine (`Claude Code`) — *Completed 2026-11-24*
- [x] TASK-1.3: Data Quality & Schema Conformance Tests (`Codex`) — *Completed 2026-11-24*
- [x] TASK-1.4: Edge-Case Anomaly Distribution & Foreign Key Tests (`Codex`) — *Completed 2026-11-24*
- [x] TASK-1.5: Independent Code Review of Generator (`Codex`) — *Completed 2026-11-24*
- [x] TASK-1.6: Phase 1 Integration Dry-Run, Verification & Regression Suite (`Antigravity`) — *Completed 2026-11-24*
- [x] TASK-1.7: Phase 1 Final Sign-Off (`Human`) — *Completed & Approved 2026-11-26*
