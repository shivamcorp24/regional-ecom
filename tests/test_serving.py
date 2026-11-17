"""Tests for the staging -> serving swap.

They need a reachable PostgreSQL with the schema migrated (``alembic upgrade head``) and skip
otherwise. Each test seeds its own staging rows inside a transaction and rolls back at the end,
so they neither depend on a previous pipeline run nor touch real data.
"""

import uuid
from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from core.database import SessionLocal
from database.repository.serving import ServingRepository

CUSTOMER = str(uuid.uuid4())
PRODUCT = str(uuid.uuid4())


@pytest.fixture
def session():
    s = SessionLocal()
    try:
        s.execute(text("SELECT count(*) FROM stg_orders"))
        s.execute(text("SELECT count(*) FROM fact_orders"))
    except DBAPIError:
        s.close()
        pytest.skip("PostgreSQL not reachable or schema not migrated")
    yield s
    s.rollback()  # everything below happens inside this one transaction
    s.close()


def _seed_staging(s, orders: list[dict]) -> None:
    """Replaces staging contents with one customer, one product, one date and the given orders."""
    s.execute(text("TRUNCATE stg_customers, stg_products, stg_date, stg_orders"))
    s.execute(text(
        "INSERT INTO stg_customers VALUES (:id, 'A', 'B', '555', 'Chicago', 'IL', 'Midwest', 'USA')"
    ), {"id": CUSTOMER})
    s.execute(text("INSERT INTO stg_products VALUES (:id, 'Mouse', 'Electronics', 10.00)"), {"id": PRODUCT})
    s.execute(text("INSERT INTO stg_date VALUES ('20250105', :d, 2025, 1, 1, 5, 'Sunday')"), {"d": date(2025, 1, 5)})
    for o in orders:
        s.execute(text(
            "INSERT INTO stg_orders VALUES (:order_id, :customer_id, :product_id, '20250105', :ts,"
            " :quantity, 10.00, :discount, :gross, :net)"
        ), {"customer_id": CUSTOMER, "product_id": PRODUCT, "ts": datetime(2025, 1, 5, 12, 0),
            "quantity": 2, "gross": Decimal("20.00"), "net": Decimal("18.00"), **o})


def _order(**overrides) -> dict:
    return {"order_id": str(uuid.uuid4()), "discount": Decimal("2.00"), **overrides}


def test_bulk_swap_loads_staging_rows(session):
    _seed_staging(session, [_order(), _order()])
    repo = ServingRepository(session)

    repo.truncate_serving_tables()
    loaded = repo.transfer_staging_to_serving()

    assert loaded == {"loaded_customers": 1, "loaded_products": 1, "loaded_dates": 1, "loaded_orders": 2}
    assert repo.get_serving_counts() == {
        "dim_customers": 1, "dim_products": 1, "dim_date": 1, "fact_orders": 2,
    }


def test_null_discount_defaults_to_zero(session):
    _seed_staging(session, [_order(discount=None, net=Decimal("20.00"))])
    repo = ServingRepository(session)

    repo.truncate_serving_tables()
    repo.transfer_staging_to_serving()

    assert session.scalar(text("SELECT discount FROM fact_orders")) == Decimal("0.00")


def test_swap_replaces_previous_contents(session):
    repo = ServingRepository(session)
    _seed_staging(session, [_order(), _order(), _order()])
    repo.truncate_serving_tables()
    repo.transfer_staging_to_serving()

    _seed_staging(session, [_order()])  # next load has fewer orders
    repo.truncate_serving_tables()
    repo.transfer_staging_to_serving()

    assert repo.get_serving_counts()["fact_orders"] == 1


def test_failed_swap_rolls_back_and_keeps_previous_data(session):
    repo = ServingRepository(session)
    _seed_staging(session, [_order(), _order()])
    repo.truncate_serving_tables()
    repo.transfer_staging_to_serving()
    before = repo.get_serving_counts()
    assert before["fact_orders"] == 2

    # Next load contains an order that points at a customer that does not exist.
    _seed_staging(session, [_order(), _order(customer_id=str(uuid.uuid4()))])

    # Run the swap in a savepoint, the way a failed transaction would roll back as a whole.
    with pytest.raises(IntegrityError):
        with session.begin_nested():
            repo.truncate_serving_tables()
            repo.transfer_staging_to_serving()

    assert repo.get_serving_counts() == before
