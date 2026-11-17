import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, OperationalError

from core.database import SessionLocal
from database.repository.serving import ServingRepository


@pytest.fixture
def session():
    s = SessionLocal()
    try:
        s.execute(text("SELECT 1"))
    except OperationalError:
        s.close()
        pytest.skip("PostgreSQL not reachable")
    yield s
    s.rollback()
    s.close()


def test_failed_swap_leaves_serving_tables_unchanged(session):
    repo = ServingRepository(session)
    before = repo.get_serving_counts()
    if before["fact_orders"] == 0:
        pytest.skip("serving tables empty; run the pipeline first")

    # Staged order that references a customer that does not exist -> FK violation mid-swap.
    session.execute(text(
        "INSERT INTO stg_orders (order_id, customer_id, product_id, date_key, order_ts, quantity,"
        " unit_price, discount, gross_revenue, net_revenue)"
        " SELECT :oid, :cid, product_id, date_key, order_ts, quantity, unit_price, discount,"
        " gross_revenue, net_revenue FROM stg_orders LIMIT 1"
    ), {"oid": str(uuid.uuid4()), "cid": str(uuid.uuid4())})

    with pytest.raises(IntegrityError):
        repo.truncate_serving_tables()
        repo.transfer_staging_to_serving()
    session.rollback()

    assert repo.get_serving_counts() == before


def test_bulk_swap_matches_staging_counts(session):
    repo = ServingRepository(session)
    staged = {t: session.scalar(text(f"SELECT count(*) FROM {t}")) for t in
              ("stg_customers", "stg_products", "stg_date", "stg_orders")}
    if staged["stg_orders"] == 0:
        pytest.skip("staging empty; run the pipeline first")

    repo.truncate_serving_tables()
    loaded = repo.transfer_staging_to_serving()
    assert loaded == {
        "loaded_customers": staged["stg_customers"],
        "loaded_products": staged["stg_products"],
        "loaded_dates": staged["stg_date"],
        "loaded_orders": staged["stg_orders"],
    }
    session.rollback()  # leave the real serving data untouched
