# Learning roadmap: regional-ecom as a data-engineering playground

## Context
Goal of the project: **learn foundational data engineering concepts and the standard tools behind them**, not ship a lean product. So the selection rule is: each phase introduces one or two concepts plus the industry-standard tool for them, and the tool is worth adding even if hand-rolling would be lighter.

Baseline (from code exploration): Bronze -> Silver (Delta, key-based incremental, retryable quarantine) -> Gold marts; Postgres star schema via full-reload staging; Spark-vs-Postgres reconciliation; Airflow + Spark standalone + Postgres in Docker. Already Delta (not plain Parquet). Dimensions are Type 1. No CI, alerting, lineage, contracts, statistical DQ, Postgres partitioning, or DAG/transform tests. The two DAGs are unlinked.

Each phase: **Concept -> Tool -> What you build -> Where it hooks in -> What you will have learned**.

---

## Phase 1: Engineering foundations (CI, tests, metrics)
- **Concepts:** CI/CD, testing pyramid for data pipelines, idempotency, run metadata.
- **Tools:** GitHub Actions, pytest, ruff, sqlfluff, Airflow `DagBag` tests, Docker build in CI.
- **Build:** `.github/workflows/ci.yml` (lint, unit tests with Postgres service container, DAG import test, `alembic upgrade/downgrade`); table-driven tests for every reject code in `jobs/silver/transforms/quality.py`; idempotency test (run Silver twice, same output); a `pipeline_run_metrics` table every job appends to; link the two DAGs with **Airflow Datasets**; clean up unused files.
- **Learned:** how data code gets tested and gated, and the metadata every later phase relies on.

## Phase 2: Data modeling depth (Postgres + warehouse theory)
- **Concepts:** SCD Type 1/2, per-attribute SCD choice, late-arriving facts/inferred dims, transaction vs periodic vs accumulating facts, partitioning/indexing.
- **Tools:** Delta `MERGE`, Alembic, Postgres declarative partitioning, BRIN/composite indexes, `EXPLAIN ANALYZE`, materialized views.
- **Build:**
  - SCD2 `dim_customers` (valid_from/to, is_current, row_hash) in `jobs/silver/transforms/dimensions.py`; generator "customer moves" mode.
  - Inferred-member handling beside quarantine for late dims.
  - Partition-scoped Gold refresh (`replaceWhere`) instead of full overwrite; event time vs `ingest_ts`.
  - Order-lifecycle feed (`order_events.csv`) and an accumulating-snapshot `fact_order_lifecycle`, plus a periodic daily snapshot.
  - Range-partitioned `fact_orders`, measured index tuning, before/after in `docs/perf/`.
- **Learned:** the Kimball topics interviews actually probe, with measured evidence.

## Phase 3: Data quality, contracts and dead-letter replay
- **Concepts:** data contracts, schema drift, expectation suites, statistical/anomaly detection, DLQ replay.
- **Tools:** **Great Expectations** (or Soda) for expectations and data docs; pydantic/YAML contracts (`contracts/`); Delta constraints.
- **Build:** versioned contract per feed validated at `core/landing.py` (additive drift = warn, breaking = fail); GE suites on Silver; batch profile job (row-count z-score vs trailing runs, null rate, PSI drift on price/quantity) writing a `dq_metrics` table; `reprocess_quarantine` DAG with reason/date filters, dry-run, `max_attempts`, audit columns; generator `--inject-anomaly` flag.
- **Learned:** the difference between row rules, contract checks and statistical monitoring, and how to drain a quarantine safely.

## Phase 4: State, incremental processing and table formats
- **Concepts:** watermarks, high-water marks, backfills, CDC, table-format features.
- **Tools:** Delta CDF, time travel/`RESTORE`, `VACUUM`, `mergeSchema`; **Apache Iceberg** side-by-side; optionally **dbt-postgres** for the Gold/serving SQL layer.
- **Build:** `pipeline_state` table (processed files + checksums, last `ingest_ts`); Bronze reads only new files; Silver watermark with lookback window; `--from/--to` backfill parameters; incremental serving load via Silver CDF and `INSERT ... ON CONFLICT` (replaces `TRUNCATE ... CASCADE` in `database/repository/serving.py`, and fixes the half-updated staging gap); an Iceberg copy of Silver orders with a short Delta-vs-Iceberg comparison (snapshots, partition evolution, engine support).
- **Learned:** how real incremental loads and replays work, and an informed Delta vs Iceberg opinion.

## Phase 5: Streaming evolution
- **Concepts:** event log, offsets, checkpoints, event-time watermarks, exactly-once semantics, batch-vs-stream trade-offs.
- **Tools:** **Redpanda (Kafka API)** container; **Spark Structured Streaming**; optionally Schema Registry + Avro.
- **Build:** producer also publishes order events to a topic; streaming Bronze with checkpointing, first with `trigger(availableNow=True)` (batch-style), then continuous; `dropDuplicatesWithinWatermark`; late-event handling; keep the CSV path as the replay source.
- **Learned:** the Kafka mental model and how it maps onto the existing medallion flow.

## Phase 6: Observability, lineage and performance
- **Concepts:** freshness/volume SLAs, alerting, lineage, benchmarking.
- **Tools:** Airflow SLAs/callbacks, Slack webhook or MailHog; **Grafana** (or Metabase/Superset) over the metrics tables; **OpenLineage + Marquez**; Spark UI/History Server.
- **Build:** freshness and volume check task; failure callbacks; dashboards over `pipeline_run_metrics` and `dq_metrics`; OpenLineage Airflow provider and Spark listener feeding Marquez; benchmark harness at 10k / 100k / 1M / 5M rows tuning `shuffle.partitions`, AQE, broadcast joins, Z-order, with results in `docs/perf/`.
- **Learned:** operating a pipeline, not just writing it.

## Optional later
Terraform/IaC for the compose stack, secrets management (replace default `postgres/postgres`), a catalog (Unity/Hive metastore/Nessie), Kubernetes-based Spark.

---

## New containers by phase
P1-P3: none. P4: none (Iceberg is a Spark package). P5: Redpanda (+ optional Schema Registry). P6: Marquez, Grafana, MailHog.

## Critical files
`jobs/silver/transforms/dimensions.py`, `jobs/silver/transforms/quality.py`, `jobs/silver/pipeline.py`, `jobs/bronze/csv_to_bronze.py`, `jobs/gold/pipeline.py`, `database/repository/serving.py`, `core/landing.py`, `dags/ecom_medallion_dag.py`, `dags/ecom_dag_utils.py`, `services/data_generator/generator.py`, `src/reconcile.py`, `src/spark/session.py`, new `alembic/versions/*`, `.github/workflows/ci.yml`, `contracts/`.

## Verification (per phase)
- CI green; new tests pass.
- Idempotency: run the DAG twice on the same input, compare checksums and row counts; `reconcile.py` still passes.
- SCD2: after "moves" mode, exactly one `is_current` row per customer, history preserved.
- Anomaly injection triggers the DQ warn/fail; replay drains quarantine and `quarantine + silver` counts still sum to Bronze.
- Failure drill: kill a task mid-run, confirm no half-state and a clean re-run.
- Streaming: restart the stream, confirm no duplicates from checkpoint recovery.
- Perf: `EXPLAIN ANALYZE` and benchmark tables recorded in `docs/perf/`.
