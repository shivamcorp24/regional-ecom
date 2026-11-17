from decimal import Decimal

from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from jobs.gold.transforms.top_customers import compute_top_customers_per_region

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("net_revenue", DecimalType(14, 2)),
])
CUSTOMERS_SCHEMA = StructType([
    StructField(n, StringType())
    for n in ("customer_id", "customer_uuid", "first_name", "last_name", "city", "state", "region")
])


def _customer(cid: str, uuid: str, region: str = "West") -> tuple:
    return (cid, uuid, f"F{cid}", f"L{cid}", "City", "ST", region)


def test_ties_are_broken_by_customer_uuid_not_customer_id(spark):
    # Customer "1" has the HIGHER uuid, customer "2" the lower one, and both spend the same.
    # Postgres ranks ties by uuid, so the Spark ranking must do the same or reconciliation drifts.
    customers = spark.createDataFrame(
        [_customer("1", "bbbbbbbb-0000-0000-0000-000000000000"),
         _customer("2", "aaaaaaaa-0000-0000-0000-000000000000")],
        CUSTOMERS_SCHEMA,
    )
    orders = spark.createDataFrame(
        [("o1", "1", Decimal("100.00")), ("o2", "2", Decimal("100.00"))], ORDERS_SCHEMA
    )

    rows = {r["customer_uuid"]: r["regional_rank"]
            for r in compute_top_customers_per_region(orders, customers).collect()}

    assert rows["aaaaaaaa-0000-0000-0000-000000000000"] == 1
    assert rows["bbbbbbbb-0000-0000-0000-000000000000"] == 2


def test_higher_spend_wins_and_regions_rank_independently(spark):
    customers = spark.createDataFrame(
        [_customer("1", "u1", "West"), _customer("2", "u2", "West"), _customer("3", "u3", "South")],
        CUSTOMERS_SCHEMA,
    )
    orders = spark.createDataFrame(
        [("o1", "1", Decimal("10.00")), ("o2", "2", Decimal("50.00")), ("o3", "3", Decimal("5.00"))],
        ORDERS_SCHEMA,
    )

    ranks = {(r["region"], r["customer_uuid"]): r["regional_rank"]
             for r in compute_top_customers_per_region(orders, customers).collect()}

    assert ranks == {("West", "u2"): 1, ("West", "u1"): 2, ("South", "u3"): 1}
