# Databricks Data Engineering Projects

A collection of hands-on data engineering projects built on Azure Databricks, covering batch and streaming ingestion, medallion architecture, Slowly Changing Dimensions, window functions, and Delta Lake internals — built while transitioning from a QA/testing background into data engineering.

**Tech stack:** Azure Databricks · Delta Lake · PySpark · Spark SQL · Unity Catalog · Azure Data Lake Storage Gen2 · Databricks Workflows · Databricks Autoloader

---

## Projects

### 1. Credit Card Fraud Detection — Streaming Ingestion Pipeline
`/credit-fraud-detection`

An incremental ingestion pipeline built on Databricks Autoloader, simulating a bank receiving daily transaction files and processing only new arrivals — not reprocessing the entire dataset on every run.

**Architecture:** Raw CSV files (partitioned by date) → Bronze (Autoloader streaming ingestion, checkpointed) → Silver (cleaned, validated) → Gold (fraud rate summary by category and state) → Data quality logging, orchestrated as a dependency-chained Databricks Workflow.

**Key concepts demonstrated:**
- Databricks Autoloader (`cloudFiles`) for incremental, checkpoint-aware file ingestion
- `trigger(availableNow=True)` — batch-style streaming that processes what's new, then stops
- Append vs. overwrite semantics, and why overwrite breaks incremental checkpointing
- Unity Catalog governance via Managed Identity (Access Connector, Storage Credential, External Location)
- Data quality auditing with append-mode logging and reconciliation checks

**Real debugging story:** Autoloader, unlike a standard batch CSV read, defaults to reading every column as a string rather than inferring types — a safety behavior that silently produced an all-string bronze schema. Traced through a downstream casting error back to the ingestion layer, fixed with `cloudFiles.inferColumnTypes`, and reset the checkpoint/schema location to force a clean reingestion.

**Proof of incremental behavior:** Ingested 20 days of transactions (33,653 rows), then simulated a new day's file arriving via an append-mode write. Rerunning the pipeline increased the row count by exactly one day's worth (to 36,037) — confirming Autoloader processed only the new file, not the full dataset.

---

### 2. Loyalty & Orders Analytics — SCD Type 2, Window Functions, Delta Internals
`/loyalty-analytics-project`

A customer loyalty analytics platform built around two core data engineering patterns: preserving historical change with Slowly Changing Dimension Type 2, and deriving customer behavior insights with window functions — on top of synthetically generated, deliberately versioned data.

**Architecture:** Two synthetic customer snapshots (generated with Faker, with controlled changes between them) → SCD Type 2 dimension table (built via MERGE + INSERT) → synthetic orders fact table → window function analytics (running totals, rankings, row comparisons) → Delta time travel verification → Delta maintenance (OPTIMIZE, ZORDER, VACUUM).

**Key concepts demonstrated:**
- Slowly Changing Dimension Type 2 — preserving full history instead of overwriting
- A two-step MERGE + INSERT pattern, since a single SQL `MERGE` statement cannot both update and insert for the same source row
- Window functions: running totals, `RANK()`, `DENSE_RANK()`, `LAG()` — with correct `ROWS BETWEEN` framing
- Delta Lake time travel (`VERSION AS OF`) to verify pipeline correctness against historical state
- Delta maintenance commands (`OPTIMIZE`, `ZORDER BY`, `VACUUM`) and the storage-cost vs. audit-history trade-off they represent

**Real debugging story — the "single MERGE" limitation:** A standard `MERGE INTO` statement lets each source row take exactly one path: matched or not matched, never both. For a changed customer, this is insufficient — their old record needs to be closed out (an update) *and* their new record needs to be inserted, in the same run. Solved with the standard two-step SCD2 pattern: a `MERGE` that only closes out changed records, followed by a separate `INSERT ... WHERE NOT EXISTS` that adds new current rows for both new and changed customers.

**Real debugging story — RANGE vs. ROWS window frames:** A running-total calculation showed the same flat value repeated across a customer's orders instead of climbing progressively. Root cause: several orders shared the exact same date, and Spark's default RANGE frame treats tied `ORDER BY` values as one group rather than processing them row by row. Fixed by explicitly specifying `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, and separately fixed the underlying data generation to use full timestamps instead of date-only precision.

**Verified with time travel:** Used `DESCRIBE HISTORY` and `VERSION AS OF` to query the customer dimension table exactly as it looked before the SCD2 pipeline ran, and compared it directly against the current state for a changed customer — visually confirming the old record was correctly closed out and a new current record correctly inserted.

---

## Common patterns across both projects

- **Medallion architecture** (bronze/silver/gold) as the organizing structure for every pipeline
- **Unity Catalog governance** — Access Connector + Managed Identity + Storage Credential + External Location, rather than shared storage account keys
- **Shared config notebooks** (`00_config`) to avoid repeating authentication and path setup across notebooks
- **Data quality logging** written in append mode, so pipeline runs build an auditable history over time
- **Orchestration** via Databricks Workflows, with explicit task dependencies

---

## What I'd build next

- Delta Live Tables (DLT) as a declarative alternative to hand-written bronze/silver/gold notebooks
- A CI/CD setup for deploying notebooks and Workflow definitions across dev/prod
- Row-level and column-level security in Unity Catalog for sensitive fields
