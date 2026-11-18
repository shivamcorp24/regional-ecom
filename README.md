# Regional E-Commerce Order & Revenue Analytics

An end-to-end data engineering project. It ingests a deliberately flawed e-commerce feed,
cleans it through a **Delta Lake Medallion pipeline** (Bronze / Silver / Gold) in PySpark,
loads the result into a **PostgreSQL star schema**, and proves the two stores agree with an
automated **reconciliation** step. The whole flow can run from one script or from **Airflow**.

See `business_overview.md` for a plain-language explanation of the business rules and reports, and
`regional_ecom_project_plan.md` for the module breakdown and status.

---

## Flow

```
services/data_generator  ──▶  data/{customers,products,raw_orders}.csv
                                      │
                                      ▼
BRONZE   warehouse/bronze/        raw strings + ingest_ts, source_file. Append-only;
                                  rows whose key already exists are skipped, so reruns
                                  and Airflow retries do not duplicate data.
                                      │
                                      ▼
SILVER   warehouse/silver/        dedupe, 5-format regex date parsing, UUID5 keys,
         warehouse/quarantine/    DecimalType(14,2) revenue, quality rules.
                                  Delta MERGE upserts, OPTIMIZE ZORDER on orders.
                                  Rejected rows -> quarantine with reject_reason
                                  + quality_report.json (counts are for the latest batch).
                                  Incremental: only new orders (plus retryable quarantined ones) are processed.
                                      │
                                      ▼
GOLD     warehouse/gold/          monthly_revenue, top_customers_per_region,
                                  category_share, customer_rfm_summary,
                                  product_performance
                                      │
                                      ▼
REVERSE-ETL  jobs/load_jdbc.py    1. Spark JDBC writes Silver into unconstrained stg_* tables
                                  2. one Postgres transaction: TRUNCATE serving tables, then
                                     INSERT ... SELECT (with casts) from stg_*
                                      │
                                      ▼
RECONCILE    src/reconcile.py     Delta vs Postgres: row counts, financial totals, monthly and
                                  category revenue, top spender per region.
                                  Writes warehouse/reconciliation_report.json and raises
                                  ReconciliationError on any drift.
```

### Quality rules (Silver)
An order is quarantined with the first matching reason:
`MISSING_ORDER_ID`, `INVALID_DATE_FORMAT`, `INVALID_QUANTITY`, `INVALID_PRICE`,
`NEGATIVE_DISCOUNT`, `DISCOUNT_EXCEEDS_GROSS`, `UNKNOWN_CUSTOMER_ID`, `UNKNOWN_PRODUCT_ID`.
Duplicate `order_id`s are dropped before validation. Silver is incremental: an order already in Silver
is skipped on later runs, and so is one quarantined for a defect of its own. Orders rejected as
`UNKNOWN_CUSTOMER_ID` or `UNKNOWN_PRODUCT_ID` are retried every run, so they move from quarantine to
Silver once the customer or product arrives. Quarantine is merged on `order_id` (never appended twice).

### Date formats handled
`yyyy-MM-dd`, `MM/dd/yyyy`, `dd-MM-yyyy`, `yyyy/MM/dd HH:mm:ss`, `dd/MM/yyyy HH:mm`.
The format is chosen by the *shape* of the string, because several are day/month ambiguous.

---

## PostgreSQL star schema

```
┌───────────────────────────────────────┐
│             dim_customers             │
├───────────────────────────────────────┤
│ PK  customer_id  UUID                 │
│     first_name   VARCHAR(50)  NOT NULL│
│     last_name    VARCHAR(50)  NOT NULL│
│     phone        VARCHAR(20)  NOT NULL│
│     city         VARCHAR(50)  NOT NULL│
│     state        VARCHAR(50)  NOT NULL│
│     region       VARCHAR(50)  NOT NULL│
│     country      VARCHAR(50)  NOT NULL│
└──────────────────┬────────────────────┘
                   │ 1
                   │ N
┌──────────────────┴────────────────────┐
│              fact_orders              │
├───────────────────────────────────────┤
│ PK  order_id       UUID               │
│ FK  customer_id    UUID     NOT NULL  │──▸ dim_customers.customer_id
│ FK  product_id     UUID     NOT NULL  │──▸ dim_products.product_id
│ FK  date_key       INT      NOT NULL  │──▸ dim_date.date_key
│     order_ts       TIMESTAMP NOT NULL │
│     quantity       INT      NOT NULL CHECK (> 0)
│     unit_price     NUMERIC(10,2) NOT NULL CHECK (>= 0)
│     discount       NUMERIC(10,2) NOT NULL DEFAULT 0.00 CHECK (>= 0)
│     gross_revenue  NUMERIC(14,2) NOT NULL CHECK (>= 0)
│     net_revenue    NUMERIC(14,2) NOT NULL CHECK (>= 0, <= gross_revenue)
│ ix_fact_orders_customer_id / _product_id / _date_key
└──────────────────┬────────────────────┘
                   │ N
                   │ 1
┌──────────────────┴────────────────────┐        ┌────────────────────────────────┐
│              dim_date                 │        │         dim_products           │
├───────────────────────────────────────┤        ├────────────────────────────────┤
│ PK  date_key     INT (YYYYMMDD)       │        │ PK  product_id   UUID          │
│     full_date    DATE NOT NULL UNIQUE │        │     product_name VARCHAR(100)  │
│     cal_year / cal_quarter (1-4)      │        │     category     VARCHAR(50)   │
│     cal_month (1-12) / cal_day (1-31) │        │     unit_price   NUMERIC(10,2) │
│     day_of_week  VARCHAR(15) NOT NULL │        └────────────────────────────────┘
└───────────────────────────────────────┘
```

`unit_price` is on the fact as well as the product dimension so `gross_revenue` stays
reproducible from the fact row alone. The unconstrained `stg_*` landing tables are defined in
`database/models/staging.py`.

---

## Project layout

```
alembic/                    schema migrations (the only way the schema is created)
core/
  config.py                 pydantic-settings; builds the SQLAlchemy URL
  database.py               engine, sessionmaker, get_db() context manager
  landing.py                shared landing-file check (CLI and Airflow)
dags/
  ecom_dag_utils.py         shared DAG settings and python_module_task helper
  ecom_medallion_dag.py     regional_ecom_medallion_pipeline (every 30 min)
  mock_producer_dag.py      upstream_ecom_data_producer (every 2 min, append mode)
database/
  init.sql                  creates the separate `airflow` database on first Postgres start
  models/                   SQLAlchemy ORM models + staging tables
  repository/serving.py     set-based staging -> serving swap (ORM path kept for small loads)
  repository/analytics.py   Postgres queries used by reconciliation
jobs/{bronze,silver,gold}/  pipeline.py coordinator + schema/transforms per layer
jobs/load_jdbc.py           reverse-ETL: Spark JDBC staging + serving swap
services/data_generator/    synthetic feed generator (oneshot / append / continuous)
src/reconcile.py            Delta vs Postgres parity checks
src/spark/session.py        SparkSession with Delta Lake + Postgres JDBC
tests/                      pytest: generator, landing check, Bronze, Gold tie-break (Spark, local),
                            serving swap (needs migrated Postgres; skips if it is down)
Dockerfile                  Airflow/Spark image: Java + a project virtualenv (also runs the Spark master/workers)
docker-compose.yml          Postgres, standalone Spark cluster (master + 2 workers), Airflow (init, webserver, scheduler)
run_pipeline.py             run all stages in one process
```

---

## Setup

Requirements: Python 3.11+, [uv](https://github.com/astral-sh/uv), Docker.

```bash
uv sync
cp .env.example .env     # then fill in the Airflow values below
docker compose up -d postgres
uv run alembic upgrade head
```

`.env` must define the three Airflow values that compose requires:

| Variable | How to generate |
|---|---|
| `AIRFLOW_FERNET_KEY` | `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `AIRFLOW_WEBSERVER_SECRET_KEY` | `python -c "import secrets; print(secrets.token_hex(32))"` |
| `AIRFLOW_ADMIN_PASSWORD` | any password; user name is `AIRFLOW_ADMIN_USER` (default `admin`) |

Postgres listens on host port `5434`; keep `POSTGRES_PORT=5434` and `POSTGRES_HOST=localhost` in
`.env` for host-side runs (containers use `postgres:5432` automatically). The URL in
`alembic.ini` is hardcoded and must match. `postgres:16` is pinned; changing the major version
needs `docker compose down -v`.

## Run it

### One command (host)
```bash
uv run python -m services.data_generator.run_service --mode oneshot --output-dir data
uv run python run_pipeline.py
```

### Stage by stage
```bash
uv run python -m jobs.bronze.pipeline
uv run python -m jobs.silver.pipeline
uv run python -m jobs.gold.pipeline
uv run python -m jobs.load_jdbc
uv run python -m src.reconcile
```

### Airflow
```bash
docker compose up -d            # builds the image on first run
```
Open http://localhost:8085 and sign in with the admin user from `.env`. Both DAGs are unpaused and
scheduled: `upstream_ecom_data_producer` appends a batch every 2 minutes and
`regional_ecom_medallion_pipeline` runs every 30 minutes (you can also trigger either manually):
`check_landing_data` → `migrate_warehouse` (`alembic upgrade head`) → `bronze_ingestion` → `silver_transformation` → `gold_marts` →
`reverse_etl_serving` → `parity_reconciliation`.

Notes: Spark runs on a standalone cluster (`spark-master` + 2 `spark-worker` replicas, 2 cores / 2 GB each).
Airflow tasks are the Spark driver (client mode) and reach the master at `spark://spark-master:7077`;
the master UI is http://localhost:8080. All nodes are built from the same image and mount the repo at
`/opt/spark-app`, so landing, Delta and warehouse paths are identical on the driver and the executors.
Set `SPARK_MASTER=local[*]` in `.env` to run in local mode instead; `SPARK_EXECUTOR_MEMORY` and
`SPARK_CORES_MAX` tune the application. The compose file follows the official Airflow-in-Docker layout. `database/init.sql` only runs when
the Postgres volume is first created, so an existing volume without an `airflow` database needs
`docker compose down -v`. The image keeps the project dependencies in a separate virtualenv because
Airflow 2.9 pins SQLAlchemy 1.4; keep the dependency list in the `Dockerfile` in sync with
`pyproject.toml`. The two DAGs are not linked; they run on independent schedules.

### Tests
```bash
uv run pytest
```

## Migrations

```bash
uv run alembic upgrade head          # apply
uv run alembic upgrade head --sql    # print DDL without connecting
uv run alembic downgrade base        # tear down
```

There is deliberately no `init_db()`: `create_all` would build tables without an Alembic version
stamp. `core.database.drop_db()` is the hard reset (drops tables and `alembic_version`).

## Source data

The generator writes `customers.csv`, `products.csv` and `raw_orders.csv` into `data/`
(gitignored). Defaults: 5000 orders (plus 50 duplicates) and 500 customers; `--seed` makes runs
reproducible. The feed contains duplicate rows, five date formats, NULL names/cities/discounts,
and negative quantities and prices. `--mode continuous` appends 100 orders every `--interval`
seconds. `--mode append` (used by the producer DAG) adds one unseeded batch of `--records` orders
with GUID order ids and ~5% duplicates, and only creates customers/products if missing. Business keys in the feed are plain integers; Silver derives deterministic UUID5 keys.
`src/generate_data.py` is an older standalone generator that the pipeline no longer uses.

## Reconciliation

`src/reconcile.py` compares Delta Lake with Postgres on:
1. row counts for the four serving tables
2. totals: orders, quantity, gross revenue, discount, net revenue (exact to the cent)
3. monthly net revenue
4. category revenue per year
5. rank-1 customer per region (ties broken by `customer_uuid` on both sides)

A failure raises `ReconciliationError`, which fails the Airflow task.

## Known limitations
- Each load fully replaces the serving tables (truncate and reload); there is no incremental load.
- A failed staging write can leave `stg_*` tables half-updated; the serving swap only runs after all
  four writes succeed, so serving tables are not affected.
- Postgres still uses default `postgres`/`postgres` credentials from `.env`.
