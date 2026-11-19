# Project Task Board

## Backlog (Phase 1 Ready for Implementation)
- [ ] TASK-1.2: Standardize `data/mock_generator.py` Engine (`Claude Code`) — *Blocked on TASK-1.0 completion & approval gate*
- [ ] TASK-1.3: Data Quality & Schema Conformance Tests (`Codex`) — *Blocked on TASK-1.0 completion & approval gate*
- [ ] TASK-1.4: Edge-Case Anomaly Distribution & Foreign Key Tests (`Codex`) — *Blocked on TASK-1.3*
- [ ] TASK-1.5: Independent Code Review of Generator (`Codex`) — *Blocked on TASK-1.2 & TASK-1.4*
- [ ] TASK-1.6: Phase 1 Integration Dry-Run & Handoff (`Antigravity`) — *Blocked on TASK-1.5*
- [ ] TASK-1.7: Sample Data Inspection & Phase 1 Sign-Off (`Human`) — *Blocked on TASK-1.6*

## Backlog (Phases 2–5)
- [ ] TASK-2.1: Implement PySpark Silver cleaning transformation (`src/etl/silver_clean.py`)
- [ ] TASK-2.2: Implement PySpark Gold KPI aggregations (`src/etl/gold_aggregate.py`)
- [ ] TASK-2.3: Implement Chispa tests for Silver & Gold (`tests/`)
- [ ] TASK-3.1: Provision AWS S3 bucket structure and verify IAM permissions
- [ ] TASK-3.2: Create Athena database and tables via `aws/athena_ddl.sql`
- [ ] TASK-4.1: Deploy Lambda trigger with S3 event notification
- [ ] TASK-4.2: Configure GitHub Actions secrets and test deployment workflow
- [ ] TASK-5.1: Configure Power BI Simba ODBC connection
- [ ] TASK-5.2: Build Power BI dashboards (Sales, Inventory, CLV)
- [ ] TASK-5.3: Reconcile Power BI numbers with Athena SQL query output

## In Progress
- [ ] TASK-1.0: Local Python 3.11 Environment Setup (`Human`)

## Done
- [x] TASK-1.1: Formalize Phase 1 Data Contracts & Anomaly Matrix (`Antigravity`) — *Completed 2026-11-19*
- [x] Initial documentation pack extracted & synced to `docs/` (`Antigravity`)
- [x] 2-Day phase-wise project implementation plan created (`Antigravity`)
