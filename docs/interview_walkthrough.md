# Regional E-Commerce Analytics: Project Walkthrough

A short guide to the project for an interviewer. It covers the problem, the design, the key decisions and what I would improve. Setup and run commands are in the [README](../README.md). The business rules are explained in [business_overview.md](business_overview.md).

## 1. The problem

An e-commerce feed arrives with realistic defects: duplicate orders, five different date formats, NULL names and discounts, and negative quantities and prices. The goal is to turn it into trusted revenue reports, and to prove the reports are correct.

## 2. What I built

An end-to-end pipeline:

```
Generator -> Bronze (raw) -> Silver (clean) -> Gold (marts) -> Postgres star schema -> Reconciliation
              Delta Lake      Delta Lake        Delta Lake       serving layer          Delta vs Postgres
```

| Layer | What it does |
|---|---|
| **Bronze** | Stores the raw strings plus ingest metadata. Append-only and idempotent: rows whose key already exists are skipped. |
| **Silver** | Deduplicates, parses five date formats, derives deterministic UUID5 keys and uses exact `Decimal(14,2)` money. Applies eight quality rules. Rejected rows go to a quarantine table with a `reject_reason`. |
| **Gold** | Business marts: monthly revenue, top customers per region, category share, customer RFM, product performance. |
| **Reverse ETL** | Spark JDBC writes into unconstrained staging tables. One Postgres transaction then truncates and reloads the serving tables. |
| **Reconcile** | Compares Delta with Postgres on counts, financial totals to the cent, monthly and category revenue, and the top customer per region. Any drift fails the run. |

**Stack:** PySpark, Delta Lake, PostgreSQL, SQLAlchemy and Alembic, Airflow, Docker Compose (standalone Spark cluster), pytest, uv.

## 3. Design decisions I can defend

1. **Medallion layers.** Raw data is kept untouched in Bronze, so I can replay Silver logic after a bug fix without re-ingesting.
2. **Quarantine instead of dropping.** Bad rows are never lost silently. Each has a reason, and a quality report shows the rejection breakdown for the latest batch.
3. **Idempotency everywhere.** Bronze skips existing keys, Silver uses Delta `MERGE`, quarantine merges on `order_id`, and the serving load is a single transaction. Airflow retries and reruns can't duplicate data.
4. **Incremental Silver.** Only new orders are processed. Orders rejected for an unknown customer or product are retried every run, so they move to Silver once the missing dimension row arrives. Orders with defects of their own are not retried.
5. **Date parsing by shape.** Several formats are day/month ambiguous (`MM/dd/yyyy` and `dd/MM/yyyy`), so the format is chosen from the string's pattern, not by trying parsers in turn.
6. **Exact money.** Decimal types throughout. No floats, so totals reconcile to the cent.
7. **Deterministic UUID5 keys.** The same business key always gives the same surrogate key, which makes reruns and the Spark-to-Postgres comparison stable.
8. **Schema only via Alembic migrations.** There is no `create_all`, so the schema is versioned and reversible.
9. **Constraints in the serving DB.** Primary and foreign keys and CHECKs (`net_revenue <= gross_revenue`, `quantity > 0`) act as a second line of defence behind Silver.
10. **Reconciliation as a pipeline step.** Correctness is verified automatically on every run, not just assumed.

## 4. Orchestration

Two independent Airflow DAGs:

- **Producer** (every 2 minutes) appends a batch of orders to simulate an upstream system.
- **Medallion pipeline** (every 30 minutes) runs `check_landing_data` -> `migrate_warehouse` -> `bronze_ingestion` -> `silver_transformation` -> `gold_marts` -> `reverse_etl_serving` -> `parity_reconciliation`.

Spark runs on a standalone cluster (1 master and 2 workers) in Docker. Airflow tasks act as the Spark driver.

## 5. Problems I hit and how I solved them

- **Duplicate quarantine rows.** Quarantine was appended on each run. I changed it to a `MERGE` on `order_id`.
- **Slow full Silver rebuilds.** I made Silver incremental, with `OPTIMIZE ZORDER` on orders.
- **Misleading quality report.** The rejection breakdown was computed after the writes, so it reflected the wrong state. I now compute it before the writes.
- **Airflow and SQLAlchemy conflict.** Airflow 2.9 pins SQLAlchemy 1.4, so the project dependencies live in a separate virtualenv inside the image.
- **Non-deterministic "top customer" on ties.** Ties are broken by `customer_uuid` on both the Spark and Postgres sides, so reconciliation is stable.

## 6. Known limitations and next steps

- The serving load is a full truncate and reload. The next step is an incremental upsert.
- Postgres uses default credentials. This needs a secrets manager.
- A failed staging write can leave the `stg_*` tables half-updated. The serving tables stay safe because the swap runs only after all four writes succeed.
- There is no alerting yet on quarantine rate or reconciliation failures.
- The feed is synthetic. A real source would also need schema-drift handling.

## 7. Likely interview questions

**Why Delta Lake instead of plain Parquet?**
`MERGE` upserts, ACID writes, time travel and `OPTIMIZE ZORDER`. Idempotent Silver and quarantine depend on `MERGE`.

**How do you guarantee no duplicates on retry?**
Every write is keyed. Bronze skips existing keys, Silver and quarantine `MERGE` on `order_id`, and the serving swap is one transaction.

**How do you know the numbers in Postgres are right?**
The reconciliation step compares Delta with Postgres on counts, financial totals to the cent, monthly and category revenue, and the top customer per region. A mismatch raises `ReconciliationError` and fails the Airflow task.

**Why a star schema?**
Reports are aggregations over orders by customer, product and date. A narrow fact table with conformed dimensions keeps those queries simple and fast.

**What would you change to scale this?**
Make the serving load incremental, partition Delta by date, move from the standalone cluster to Kubernetes or a managed Spark service, and add data-quality alerting.

**Why store `unit_price` on the fact table too?**
So `gross_revenue` can be reproduced from the fact row alone, even if the product's price changes later.
