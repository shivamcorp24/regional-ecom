# Project Plan: Regional E-Commerce Order & Revenue Analytics (Delta Lakehouse Edition)

## Project Overview
An end-to-end data engineering project: a flawed raw multi-region source feed, a PySpark
**Medallion pipeline** (Bronze / Silver / Gold in **Delta Lake**) with rule-based validation and
quarantine, a Spark JDBC **reverse-ETL** into a **PostgreSQL star schema**, and an automated
**reconciliation** that proves the Lakehouse and the serving store agree.

**Status: all modules complete and verified end to end** (CLI and Airflow, reconciliation PASSED).

---

## Architecture Flow
```
[ Raw CSV source: raw_orders.csv / customers.csv / products.csv ]
          │
          │  PySpark batch pipeline (host in local mode, or Docker with a standalone Spark cluster)
          ▼
  ┌─ BRONZE ─ append-only Delta tables + ingest_ts, source_file
  │      │      warehouse/bronze/  (rows with an existing key are skipped on rerun)
  │      ▼
  ├─ SILVER ─ dedupe, regex date parsing, UUID5 keys, DecimalType(14,2) revenue,
  │      │    quality rules, Delta MERGE upserts, OPTIMIZE ZORDER (orders)
  │      │      warehouse/silver/
  │      │      warehouse/quarantine/  (+ quality_report.json)
  │      ▼
  ├─ GOLD ─── monthly_revenue, top_customers_per_region, category_share,
  │      │    customer_rfm_summary, product_performance   (warehouse/gold/)
  │      ▼
  ├─ REVERSE ETL ─ Spark JDBC (batchsize=10000, 4 partitions) into stg_* tables,
  │                then one Postgres transaction: TRUNCATE + INSERT ... SELECT
  ▼
[ PostgreSQL serving store ]  ──▶  src/reconcile.py compares it with Delta
```

### Layer contract
| Layer | Content | Format | Features |
|---|---|---|---|
| **Bronze** | Raw source as strings + `ingest_ts`, `source_file` | Delta | Append-only, key-based skip of already-ingested rows |
| **Silver** | Cleaned dimensions, calendar and orders | Delta | `MERGE INTO` upserts, `DecimalType(14,2)`, Z-ORDER |
| **Quarantine** | Rejected orders with `reject_reason` | Delta | `quality_report.json` with a rejection breakdown |
| **Gold** | Analytical marts | Delta | Partitioned, compacted with `OPTIMIZE` |
| **Serving** | `dim_*`, `fact_orders` (+ unconstrained `stg_*`) | Postgres 16 | Constraints and foreign keys enforced |

---

## Module 1: Data Generation — complete
*`services/data_generator/` (`generator.py`, `run_service.py`).*

- **1.1** `raw_orders.csv` (default 5000 orders + 50 duplicates), `customers.csv`, `products.csv`.
- **1.2** Injected defects: exact duplicate rows, 5 mixed date formats (several day/month ambiguous),
  NULL names/cities/discounts, negative quantity and price.
- **1.3** `oneshot`, `append` and `continuous` modes; `append` adds an unseeded batch (GUID order ids,
  ~5% duplicates) without overwriting earlier orders and is what the producer DAG runs.
- The feed uses integer business keys; Silver derives deterministic UUID5 keys.
- `src/generate_data.py` is the original standalone generator and is no longer used.

## Module 2: Relational Serving Model (PostgreSQL) — complete
- **2.1** Star schema: `dim_customers`, `dim_products`, `dim_date`, `fact_orders`.
- **2.2** DDL through Alembic (`uv run alembic upgrade head`).
- **2.3** Reset with `alembic downgrade base` or `drop_db()`.
- **2.4** Unconstrained staging tables `stg_*` in `database/models/staging.py`.

## Module 3: Platform (Docker & Spark) — complete
- **3.1** `docker-compose.yml` (Airflow part follows the official Airflow-in-Docker layout): `postgres` (healthcheck,
  `database/init.sql` creates the `airflow` database), a standalone Spark cluster (`spark-master` and 2
  `spark-worker` replicas built from the Airflow image) and Airflow (`airflow-init`, webserver, scheduler).
- **3.2** `src/spark/session.py`: Delta Lake extension and catalog, Postgres JDBC package, UTC timezone.
- **3.3** `Dockerfile`: Airflow 2.9.3 image with Java and a separate project virtualenv
  (Airflow pins SQLAlchemy 1.4, which conflicts with the project).
- **3.4** Secrets (Airflow keys, admin password, Postgres credentials) are read from `.env`.
- Airflow tasks are the Spark driver (client mode) against `spark://spark-master:7077`; every node mounts
  the repo at `/opt/spark-app` so file paths match. `SPARK_MASTER=local[*]` falls back to local mode.

## Module 4: Medallion ETL — complete

### 4.1 Bronze (`jobs/bronze/`)
- Read CSVs with explicit string schemas, add `ingest_ts` and `source_file`.
- Append to Delta, skipping rows whose key (`order_id`, `customer_id`, `product_id`) already exists.

### 4.2 Silver (`jobs/silver/`, `transforms/`)
- Shape-based regex date parser for 5 formats (`dates.py`); dense calendar between min and max order date (extended with the dates Silver already covers).
- Customer and product cleaning with deterministic UUID5 keys (`dimensions.py`).
- `DecimalType(14,2)` gross and net revenue (`financials.py`).
- Rule-based quality checks and quarantine with `reject_reason` (`quality.py`):
  `MISSING_ORDER_ID`, `INVALID_DATE_FORMAT`, `INVALID_QUANTITY`, `INVALID_PRICE`,
  `NEGATIVE_DISCOUNT`, `DISCOUNT_EXCEEDS_GROSS`, `UNKNOWN_CUSTOMER_ID`, `UNKNOWN_PRODUCT_ID`.
  (Hand-written rules; Great Expectations is not used.)
- Incremental orders: only Bronze orders that are not in Silver and not quarantined for a final reason are
  processed (`select_unprocessed_orders`), so run time follows the batch size. Orders rejected for a missing
  customer or product (`RETRYABLE_REJECT_REASONS`) are retried each run and leave quarantine once valid.
  Quarantine is merged on `order_id`, never appended twice; writes are idempotent and ordered so a crash
  leaves an order in Silver, in quarantine, or in neither (and so reprocessed). The quality report is written last.
- Delta `MERGE INTO` upserts and `OPTIMIZE ... ZORDER BY (customer_id, order_ts)` (`upsert.py`).

### 4.3 Gold (`jobs/gold/`)
- `monthly_revenue`, `top_customers_per_region` (`DENSE_RANK` by region, ties broken by `customer_uuid`),
  `category_share`, `customer_rfm_summary`, `product_performance`.

## Module 5: Serving Load (Reverse-ETL) — complete
- **5.1** `jobs/load_jdbc.py`: DataFrames are cached and counted once, then written to `stg_*`
  with `batchsize=10000`, 4 partitions, `READ_COMMITTED`.
- **5.2** Atomic swap in `database/repository/serving.py`, one transaction:
  `TRUNCATE ... CASCADE`, then `INSERT ... SELECT` with casts (dimensions first, then facts;
  NULL discount becomes `0.00`). A failure rolls everything back. An ORM row-by-row path remains
  for small loads.

## Module 6: Reconciliation & Orchestration — complete
- **6.1** Queries for the Postgres side in `database/repository/analytics.py`.
- **6.2** `src/reconcile.py` checks row counts, financial totals, monthly and category revenue, and
  the top spender per region; writes `warehouse/reconciliation_report.json`; raises on drift.
- **6.3** Airflow DAGs in `dags/`: `regional_ecom_medallion_pipeline` (every 30 min, with a
  `migrate_warehouse` Alembic task before Bronze) and `upstream_ecom_data_producer` (every 2 min,
  append mode), sharing helpers in `ecom_dag_utils.py`.
- **6.4** `run_pipeline.py` runs all stages in one process. `core/landing.py` is the shared
  landing-file check.
- **6.5** Tests in `tests/` cover the generator, the landing check, Bronze idempotency, Silver
  incremental processing (no reprocessing, no duplicate quarantine rows, retry of orders with missing master data), the Gold
  top-customer tie-break (Spark, local, no Docker) and the serving swap (needs Postgres; skips if
  it is down). The Silver transforms, the reconciliation logic and the DAGs are not covered.

---

## Definition of Done
- [x] Delta Lake features (MERGE INTO, OPTIMIZE Z-ORDER) used in Silver and Gold.
- [x] Spark and Postgres match to the cent (reconciliation PASSED, 12 of 12 checks).
- [x] JDBC writes tuned with `batchsize=10000` and 4 partitions.
- [x] Pipeline runs with one command (`run_pipeline.py`) or through Airflow (all 6 tasks succeed).
- [x] Bronze reruns do not duplicate data.

## Open items
- The two DAGs are not linked; the pipeline DAG does not start when the producer finishes.
- Full table reload on every run; no incremental load.
- Staging tables can be left half-updated after a failed write (serving tables stay safe).
- Default `postgres`/`postgres` database credentials in `.env`.
- Unused code that could be removed: `src/generate_data.py`, `database/schema/`,
  `database/repository/base.py`, `jobs/silver/schema.py`, `jobs/gold/schema.py`.
