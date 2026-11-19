# Interview Script: Regional E-Commerce Analytics

A spoken talk track, about 6 minutes at a normal pace. Say it in your own words. Don't read it out. Supporting detail is in [interview_walkthrough.md](interview_walkthrough.md) and the [README](../README.md).

---

## 1. Opening (30 sec)

> "I built an end-to-end data engineering pipeline for a regional e-commerce business. It takes a messy order feed, cleans it with PySpark and Delta Lake, loads it into a PostgreSQL star schema, and then automatically proves the two stores agree. The whole thing runs on Airflow, with Spark on a Docker cluster."

## 2. The problem (30 sec)

> "The source data is deliberately flawed, like a real feed would be. It has duplicate orders, five different date formats, NULL names and discounts, and negative quantities and prices. The business needs revenue reports it can trust, so the challenge was not just moving data. It was cleaning it safely and being able to prove the results are correct."

## 3. Architecture (90 sec)

*Sketch or point at the diagram in the README.*

> "I used the medallion pattern with three layers.
>
> **Bronze** holds the raw data exactly as it arrived, plus ingest metadata. It's append-only, and rows whose key already exists are skipped, so a rerun can't duplicate anything.
>
> **Silver** is where the cleaning happens. I deduplicate, parse the five date formats, derive deterministic UUID keys, and use exact decimal types for money. Eight quality rules decide whether an order is valid. Invalid orders aren't dropped. They go to a quarantine table with a reject reason, so nothing disappears silently.
>
> **Gold** has the business marts: monthly revenue, top customers per region, category share, customer RFM and product performance.
>
> From there, a reverse-ETL step loads Silver into a Postgres star schema with one fact table and three dimensions. Spark writes to staging tables first. Then a single transaction truncates and reloads the serving tables, so readers never see a half-loaded state.
>
> The last step is reconciliation. It compares Delta against Postgres on row counts, financial totals to the cent, monthly and category revenue, and the top customer per region. If anything drifts, the pipeline fails."

## 4. Decisions I'm proud of (2 min)

Pick three or four, depending on time and where the interviewer leans.

**Idempotency**
> "Every write is keyed, so retries are safe. Bronze skips existing keys, Silver and quarantine use Delta MERGE, and the serving load is one transaction. In Airflow, tasks do get retried, so this wasn't optional."

**Quarantine, not deletion**
> "Rejected rows keep a reason code and appear in a quality report. Orders rejected for an unknown customer or product are retried every run, because the customer might simply arrive later. Orders with a defect of their own aren't retried."

**Date parsing by shape**
> "Formats like MM/dd/yyyy and dd/MM/yyyy are ambiguous, so trying parsers in order would silently misread dates. I pick the format from the string's pattern instead."

**Exact money**
> "Decimal types everywhere, never floats. That's what lets reconciliation match to the cent."

**Reconciliation as a pipeline step**
> "I didn't want to assume the numbers were right. The check runs on every run, and a mismatch fails the Airflow task."

## 5. A problem I hit (60 sec)

Choose one. The first is the strongest.

**Option A: duplicate quarantine rows**
> "Early on, quarantine was appended on every run, so rerunning the pipeline duplicated the rejected rows. I changed it to a MERGE on order_id. At the same time I made Silver incremental, so each run only processes new orders plus the retryable quarantined ones. That fixed both the duplicates and the slow full rebuilds."

**Option B: the quality report**
> "The rejection breakdown in the quality report was computed after the writes, so it reflected the wrong state. I moved it to run before the writes, so the report matches the batch that was just processed."

## 6. Limitations and next steps (30 sec)

> "The serving load is a full truncate and reload. At larger scale I'd make it an incremental upsert. Credentials are still defaults and need a secrets manager. There's no alerting yet on quarantine rate. And the feed is synthetic, so a real source would also need schema-drift handling."

## 7. Close (15 sec)

> "The main thing I took from this is that correctness has to be designed in: idempotent writes, quarantine instead of deletion, and automated reconciliation. Happy to go deeper on any part."

---

## Quick answers to likely follow-ups

| Question | Short answer |
|---|---|
| Why Delta over Parquet? | MERGE upserts, ACID writes, ZORDER. Idempotent Silver depends on MERGE. |
| How do you avoid duplicates on retry? | Every write is keyed: skip-if-exists in Bronze, MERGE in Silver and quarantine, a single transaction for serving. |
| How do you know Postgres is right? | The reconciliation step. Counts, totals to the cent, monthly and category revenue, top customer per region. |
| Why a star schema? | Reports aggregate orders by customer, product and date. A narrow fact with dimensions keeps queries simple. |
| Why is `unit_price` on the fact table? | So `gross_revenue` can be reproduced from the fact row even if the product price changes. |
| How would you scale it? | Incremental serving load, partition Delta by date, move to Kubernetes or managed Spark, add alerting. |
| Why UUID5 keys? | Deterministic: the same business key always gives the same surrogate key, so reruns and comparisons are stable. |
