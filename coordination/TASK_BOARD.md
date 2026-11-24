# Project Task Board

## Backlog (Phase 2: PySpark Medallion ETL Ready to Start)
- [ ] TASK-2.1: Implement PySpark Silver cleaning transformation (`src/etl/silver_clean.py`)
- [ ] TASK-2.2: Implement PySpark Gold KPI aggregations (`src/etl/gold_aggregate.py`)
- [ ] TASK-2.3: Implement Chispa tests for Silver & Gold (`tests/`)

## Backlog (Phases 3–5)
- [ ] TASK-3.1: Provision AWS S3 bucket structure and verify IAM permissions
- [ ] TASK-3.2: Create Athena database and tables via `aws/athena_ddl.sql`
- [ ] TASK-4.1: Deploy Lambda trigger with S3 event notification
- [ ] TASK-4.2: Configure GitHub Actions secrets and test deployment workflow
- [ ] TASK-5.1: Configure Power BI Simba ODBC connection
- [ ] TASK-5.2: Build Power BI dashboards (Sales, Inventory, CLV)
- [ ] TASK-5.3: Reconcile Power BI numbers with Athena SQL query output

## In Progress
*(No tasks currently in progress — Phase 1 completed, ready for Phase 2 initiation)*

## Done (Phase 1: Local Development & Synthetic Data Engine)
- [x] TASK-1.0: Local Python 3.11 Virtual Environment & Dependency Setup (`Human`) — *Completed 2026-11-23*
- [x] TASK-1.1: Formalize Phase 1 Data Contracts & Anomaly Matrix (`Antigravity`) — *Completed 2026-11-19*
- [x] TASK-1.2: Standardize `data/mock_generator.py` Engine (`Claude Code`) — *Completed 2026-11-24*
- [x] TASK-1.3: Data Quality & Schema Conformance Tests (`Codex`) — *Completed 2026-11-24*
- [x] TASK-1.4: Edge-Case Anomaly Distribution & Foreign Key Tests (`Codex`) — *Completed 2026-11-24*
- [x] TASK-1.5: Independent Code Review of Generator (`Codex`) — *Completed 2026-11-24*
- [x] TASK-1.6: Phase 1 Integration Dry-Run, Verification & Regression Suite (`Antigravity`) — *Completed 2026-11-24*
- [x] TASK-1.7: Phase 1 Sign-Off Completed (Subject to final human confirmation) — *Completed 2026-11-24*
