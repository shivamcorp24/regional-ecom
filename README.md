# Regional E-Commerce Order & Revenue Analytics

End-to-end data engineering project: generate a deliberately flawed source
dataset, model it as a PostgreSQL star schema, then ETL it and analyse it.

See `regional_ecom_project_plan.md` for the full module breakdown.

**Status:** Modules 1 (data generation) and 2 (relational modeling) complete.
Modules 3-5 (extract/transform/load, auditing, analytics) not yet written.

---

## Star schema

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
                   │
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
│     net_revenue    NUMERIC(14,2) NOT NULL CHECK (>= 0)
│                                       CHECK (<= gross_revenue)
│                                       │
│ ix_fact_orders_customer_id            │
│ ix_fact_orders_product_id             │
│ ix_fact_orders_date_key               │
└──────────────────┬────────────────────┘
                   │ N
                   │
                   │ 1
┌──────────────────┴────────────────────┐        ┌────────────────────────────────┐
│              dim_date                 │        │         dim_products          │
├───────────────────────────────────────┤        ├────────────────────────────────┤
│ PK  date_key     INT (YYYYMMDD)       │        │ PK  product_id   UUID          │
│     full_date    DATE NOT NULL UNIQUE │        │     product_name VARCHAR(100)  │
│     cal_year     INT         NOT NULL │        │     category     VARCHAR(50)   │
│     cal_quarter  INT         NOT NULL │        │     unit_price   NUMERIC(10,2) │
│                   CHECK (BETWEEN 1-4) │        │                   CHECK (>= 0)  │
│     cal_month    INT         NOT NULL │        └────────────────────────────────┘
│                   CHECK (BETWEEN 1-12)│
│     cal_day      INT         NOT NULL │
│                   CHECK (BETWEEN 1-31)│
│     day_of_week  VARCHAR(15)  NOT NULL│
└───────────────────────────────────────┘
```

`unit_price` is carried on the fact as well as the product dimension so a
per-order price override is preserved and `gross_revenue` stays reproducible
from the fact row alone.

---

## Layout

```
core/config.py            pydantic-settings; builds the SQLAlchemy URL via URL.create
core/database.py          engine, sessionmaker, get_db() contextmanager, drop_db(), re-exports Base
database/models/          SQLAlchemy ORM (the single DeclarativeBase lives in base.py)
database/schema/          Pydantic DTOs mirroring the ORM
database/repository/      generic DAL over an injected Session
alembic/                  migrations; connection URL lives in alembic.ini
src/generate_data.py      Module 1 generator
data/                     generated source CSVs (gitignored)
```

---

## Setup

```bash
uv sync
cp .env.example .env          # adjust credentials if needed
docker compose up -d
```

The compose file maps host `5434` to container `5432`; `POSTGRES_PORT` in `.env`
must match the host side. There is no healthcheck, so `up -d` returns as soon as
the container starts rather than when Postgres is accepting connections. If
`alembic upgrade head` fails to connect on a cold start, wait a second and retry.

The `pgdata` volume is owned by the image's Postgres major version, which is
pinned to `postgres:16`. Changing that tag needs `docker compose down -v`
first, otherwise the server exits instead of starting on the old data
directory.

## Migrations

```bash
uv run alembic upgrade head          # apply
uv run alembic upgrade head --sql    # print DDL without connecting
uv run alembic downgrade base        # tear down (plan Task 2.3 reset)
```

The connection URL is set in `alembic.ini` under `sqlalchemy.url` and must match
the host/port in `.env`. The `database/models/__init__.py` import is what
registers the tables on `Base.metadata` for `--autogenerate`.

Alembic is the only way the schema is created. There is deliberately no
`init_db()`: `create_all` would build tables without a version stamp, so the
next `upgrade head` would fail on `CREATE TABLE` against tables that already
exist. `core.database.drop_db()` is the hard reset - it drops the tables *and*
the `alembic_version` stamp, leaving the same pre-Alembic state as
`downgrade base`.

## Generate source data

```bash
uv run python -m src.generate_data                        # 1,000 orders
uv run python -m src.generate_data --records 50000        # bigger fixture
uv run python -m src.generate_data --seed 7 --customers 200
```

Writes `data/customers.csv`, `data/products.csv`, `data/raw_orders.csv`.
Keys are deterministic `uuid5` values, so a given `--seed` always reproduces
byte-identical files - which is what lets the eventual load be idempotent.

The order feed is deliberately flawed (plan Task 1.2): duplicate rows, five
mixed date formats, NULL customer/city/product/discount fields, and negative
quantities and prices. `--help` lists every flag.

---

## Notes for the next module

Open decisions that block the transform stage:

1. **Negative values.** `quantity < 0` and `unit_price < 0` both violate the
   new CHECK constraints. Drop, `abs()`, clamp, or quarantine the rows?
2. **Ambiguous dates.** Three of the five formats are day/month ambiguous, so
   `pd.to_datetime(..., format="mixed")` silently mis-parses them. Parsing has
   to be per-format, but which format a given value used is not recorded - the
   only unambiguous signal is the shape of the string.
3. **Missing time-of-day.** Two of the five formats carry no time, so
   `order_ts` has to default to `00:00:00` for those rows.
4. **Denormalized copies.** The feed repeats customer name/city and product
   name/category. They agree with the masters except where NULLed, so the
   master is the safer source; decide whether to reconcile or trust the feed.
