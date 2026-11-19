"""Silver processes only new orders and never re-appends quarantined rows."""

import pandas as pd

from jobs.bronze.pipeline import run_bronze_pipeline
from jobs.silver.pipeline import (
    calendar_bounds,
    run_silver_pipeline,
    select_unprocessed_orders,
    upsert_quarantine,
)
from services.data_generator.generator import generate_all, generate_incremental_batch


def _ids(df) -> set:
    return {r["order_id"] for r in df.select("order_id").collect()}


def test_select_unprocessed_orders_skips_silver_and_quarantine_keys(spark, tmp_path):
    silver_dir, quarantine_dir = str(tmp_path / "silver"), str(tmp_path / "quarantine")
    spark.createDataFrame([("1",)], "order_id string").write.format("delta").save(f"{silver_dir}/orders")
    spark.createDataFrame(
        [("2", "INVALID_PRICE"), (None, "MISSING_ORDER_ID")], "order_id string, reject_reason string"
    ).write.format("delta").save(f"{quarantine_dir}/orders")
    bronze = spark.createDataFrame([("1",), ("2",), ("3",), (None,)], "order_id string")

    result = select_unprocessed_orders(spark, bronze, silver_dir, quarantine_dir)

    assert _ids(result) == {"3"}  # includes the null-id row already quarantined


def test_select_unprocessed_orders_retries_only_master_data_rejections(spark, tmp_path):
    quarantine_dir = str(tmp_path / "quarantine")
    spark.createDataFrame(
        [("1", "INVALID_PRICE"), ("2", "UNKNOWN_CUSTOMER_ID"), ("3", "UNKNOWN_PRODUCT_ID")],
        "order_id string, reject_reason string",
    ).write.format("delta").save(f"{quarantine_dir}/orders")
    bronze = spark.createDataFrame([("1",), ("2",), ("3",), ("4",)], "order_id string")

    result = select_unprocessed_orders(spark, bronze, str(tmp_path / "silver"), quarantine_dir)

    assert _ids(result) == {"2", "3", "4"}  # order 1 has a defect of its own and stays quarantined


def test_upsert_quarantine_updates_reason_instead_of_duplicating(spark, tmp_path):
    path = str(tmp_path / "quarantine" / "orders")
    schema = "order_id string, reject_reason string"
    upsert_quarantine(spark, path, spark.createDataFrame([("1", "UNKNOWN_CUSTOMER_ID")], schema))
    upsert_quarantine(
        spark, path, spark.createDataFrame([("1", "INVALID_PRICE"), ("2", "INVALID_QUANTITY")], schema)
    )

    rows = {r["order_id"]: r["reject_reason"] for r in spark.read.format("delta").load(path).collect()}

    assert rows == {"1": "INVALID_PRICE", "2": "INVALID_QUANTITY"}


def test_select_unprocessed_orders_returns_everything_on_first_run(spark, tmp_path):
    bronze = spark.createDataFrame([("1",), ("2",)], "order_id string")

    result = select_unprocessed_orders(spark, bronze, str(tmp_path / "s"), str(tmp_path / "q"))

    assert _ids(result) == {"1", "2"}


def test_calendar_bounds_extend_existing_calendar(spark, tmp_path):
    silver_dir = str(tmp_path / "silver")
    spark.createDataFrame([(20250101,), (20250110,)], "date_key int").write.format("delta").save(f"{silver_dir}/date")
    orders = spark.createDataFrame([("2025-02-01 10:00:00",)], "order_ts string").selectExpr("to_timestamp(order_ts) as order_ts")

    start, end = calendar_bounds(spark, orders, silver_dir)

    assert (str(start), str(end)) == ("2025-01-01", "2025-02-01")  # existing start kept, end from batch


def test_rerun_does_no_work_and_new_batch_only_processes_new_rows(spark, tmp_path):
    data_dir, warehouse = str(tmp_path / "data"), str(tmp_path / "warehouse")
    silver_kwargs = {
        "bronze_dir": f"{warehouse}/bronze",
        "silver_dir": f"{warehouse}/silver",
        "quarantine_dir": f"{warehouse}/quarantine",
    }
    quarantine_path = f"{warehouse}/quarantine/orders"

    generate_all(records=200, customers=20, output_dir=data_dir, seed=1)
    run_bronze_pipeline(spark, data_dir=data_dir, warehouse_dir=warehouse)
    first = run_silver_pipeline(spark, **silver_kwargs)

    assert first["new_orders_count"] == first["valid_orders_count"] + first["quarantined_orders_count"] > 0
    assert first["quarantined_orders_count"] > 0  # the feed has injected defects
    report = first["quality_report"]
    assert report["quarantined_records"] == first["quarantined_orders_count"]
    assert sum(report["rejection_breakdown"].values()) == first["quarantined_orders_count"]
    quarantine = spark.read.format("delta").load(quarantine_path)
    assert quarantine.count() == quarantine.select("order_id").distinct().count()
    quarantined_after_first = quarantine.count()
    silver_after_first = spark.read.format("delta").load(f"{warehouse}/silver/orders").count()

    # Re-running with no new data must not touch Silver or the quarantine.
    second = run_silver_pipeline(spark, **silver_kwargs)
    assert second["new_orders_count"] == 0
    assert spark.read.format("delta").load(quarantine_path).count() == quarantined_after_first
    assert spark.read.format("delta").load(f"{warehouse}/silver/orders").count() == silver_after_first

    # A new batch is the only thing processed on the next run.
    generate_incremental_batch(records=100, customers=20, output_dir=data_dir)
    run_bronze_pipeline(spark, data_dir=data_dir, warehouse_dir=warehouse)
    third = run_silver_pipeline(spark, **silver_kwargs)

    assert third["new_orders_count"] == 100
    final_quarantine = spark.read.format("delta").load(quarantine_path)
    assert final_quarantine.count() == final_quarantine.select("order_id").distinct().count()
    silver_orders = spark.read.format("delta").load(f"{warehouse}/silver/orders")
    assert silver_orders.count() == silver_after_first + third["valid_orders_count"]


def test_orders_quarantined_for_missing_customer_are_retried_and_graduate(spark, tmp_path):
    data_dir, warehouse = str(tmp_path / "data"), str(tmp_path / "warehouse")
    silver_kwargs = {
        "bronze_dir": f"{warehouse}/bronze",
        "silver_dir": f"{warehouse}/silver",
        "quarantine_dir": f"{warehouse}/quarantine",
    }
    quarantine_path = f"{warehouse}/quarantine/orders"

    generate_all(records=200, customers=20, output_dir=data_dir, seed=1)
    customers_path = f"{data_dir}/customers.csv"
    all_customers = pd.read_csv(customers_path, dtype=str)
    orders = pd.read_csv(f"{data_dir}/raw_orders.csv", dtype=str)
    missing_customer = orders["customer_id"].mode()[0]

    # The customer is missing when the orders first arrive.
    all_customers[all_customers["customer_id"] != missing_customer].to_csv(customers_path, index=False)
    run_bronze_pipeline(spark, data_dir=data_dir, warehouse_dir=warehouse)
    run_silver_pipeline(spark, **silver_kwargs)

    def unknown_customer_ids():
        quarantine = spark.read.format("delta").load(quarantine_path)
        return _ids(quarantine.filter("reject_reason = 'UNKNOWN_CUSTOMER_ID'"))

    waiting = unknown_customer_ids()
    assert waiting
    silver_before = spark.read.format("delta").load(f"{warehouse}/silver/orders").count()

    # Still missing: the retry must not duplicate the quarantine rows or change Silver.
    still_missing = run_silver_pipeline(spark, **silver_kwargs)
    assert still_missing["new_orders_count"] == len(waiting)
    assert unknown_customer_ids() == waiting
    assert spark.read.format("delta").load(f"{warehouse}/silver/orders").count() == silver_before

    # The customer arrives later: the waiting orders move from quarantine to Silver.
    all_customers.to_csv(customers_path, index=False)
    run_bronze_pipeline(spark, data_dir=data_dir, warehouse_dir=warehouse)
    graduated = run_silver_pipeline(spark, **silver_kwargs)

    assert graduated["new_orders_count"] == len(waiting)
    assert graduated["valid_orders_count"] == len(waiting)
    assert unknown_customer_ids() == set()
    silver_ids = _ids(spark.read.format("delta").load(f"{warehouse}/silver/orders"))
    assert waiting <= silver_ids
    assert spark.read.format("delta").load(f"{warehouse}/silver/orders").count() == silver_before + len(waiting)

    # And nothing is retried any more.
    assert run_silver_pipeline(spark, **silver_kwargs)["new_orders_count"] == 0
