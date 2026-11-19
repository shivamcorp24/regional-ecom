"""Silver layer pipeline: cleans dimensions, generates calendar, validates orders, and upserts tables.

Orders are processed incrementally: only Bronze orders that are in neither Silver nor quarantine yet
are evaluated, so a run costs time proportional to the new batch instead of the whole history.
"""

from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, lit

from jobs.silver.transforms.dates import generate_dense_calendar, parse_mixed_dates
from jobs.silver.transforms.dimensions import clean_customers, clean_products, to_uuid5
from jobs.silver.transforms.financials import calculate_financial_metrics
from jobs.silver.transforms.quality import (
    RETRYABLE_REJECT_REASONS,
    count_rejections,
    evaluate_data_quality,
    generate_and_save_quality_report,
)
from jobs.silver.transforms.upsert import optimize_delta_table, upsert_delta_table
from src.spark.session import get_spark_session


def select_unprocessed_orders(
    spark: SparkSession,
    bronze_orders_df: DataFrame,
    silver_dir: str,
    quarantine_dir: str,
) -> DataFrame:
    """Returns Bronze orders that still need evaluating: new ones, plus retryable quarantined ones.

    An order is skipped once it is in Silver, or quarantined for a reason that re-evaluation cannot
    change. Orders quarantined for a missing customer or product (RETRYABLE_REJECT_REASONS) are
    picked up again, so they reach Silver once the master data arrives. Only the key and reason
    columns of the two target tables are read. Null-safe matching keeps rows without an order_id
    from being picked up (and quarantined) again on every run.
    """
    processed_keys: DataFrame | None = None

    silver_path = f"{silver_dir}/orders"
    if DeltaTable.isDeltaTable(spark, silver_path):
        processed_keys = spark.read.format("delta").load(silver_path).select("order_id").distinct()

    quarantine_path = f"{quarantine_dir}/orders"
    if DeltaTable.isDeltaTable(spark, quarantine_path):
        final_rejects = (
            spark.read.format("delta").load(quarantine_path)
            .filter(~coalesce(col("reject_reason").isin(*RETRYABLE_REJECT_REASONS), lit(False)))
            .select("order_id")
            .distinct()
        )
        processed_keys = final_rejects if processed_keys is None else processed_keys.union(final_rejects)

    if processed_keys is None:
        return bronze_orders_df

    return bronze_orders_df.alias("b").join(
        processed_keys.alias("k"),
        col("b.order_id").eqNullSafe(col("k.order_id")),
        "left_anti",
    )


def upsert_quarantine(spark: SparkSession, path: str, rejected_df: DataFrame) -> None:
    """Inserts newly rejected orders; an order that is already quarantined only gets its reason updated."""
    if not DeltaTable.isDeltaTable(spark, path):
        rejected_df.write.format("delta").mode("append").save(path)
        return

    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(rejected_df.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdate(set={"reject_reason": "s.reject_reason"})
        .whenNotMatchedInsertAll()
        .execute()
    )


def remove_from_quarantine(spark: SparkSession, path: str, valid_ids_df: DataFrame) -> int:
    """Deletes retried orders that have become valid from quarantine; returns how many were removed.

    Only retryable quarantine rows can graduate, so the (small) set of those is joined first and the
    quarantine table is left untouched on runs where no retried order passed.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        return 0

    retryable_ids = (
        spark.read.format("delta").load(path)
        .filter(col("reject_reason").isin(*RETRYABLE_REJECT_REASONS))
        .select("order_id")
    )
    graduated_ids_df = valid_ids_df.join(retryable_ids, "order_id", "inner").distinct().cache()
    try:
        graduated = graduated_ids_df.count()
        if graduated:
            (
                DeltaTable.forPath(spark, path).alias("t")
                .merge(graduated_ids_df.alias("s"), "t.order_id = s.order_id")
                .whenMatchedDelete()
                .execute()
            )
        return graduated
    finally:
        graduated_ids_df.unpersist()


def calendar_bounds(spark: SparkSession, orders_df: DataFrame, silver_dir: str) -> tuple[Any, Any]:
    """Date range for the calendar: this batch's orders, extended by what Silver already covers."""
    batch = (
        orders_df.filter(col("order_ts").isNotNull())
        .selectExpr("min(to_date(order_ts)) as min_date", "max(to_date(order_ts)) as max_date")
        .first()
    )
    start = batch["min_date"] if batch else None
    end = batch["max_date"] if batch else None

    date_path = f"{silver_dir}/date"
    if DeltaTable.isDeltaTable(spark, date_path):
        existing = (
            spark.read.format("delta").load(date_path)
            # date_key is an integer yyyyMMdd, so it has to be parsed explicitly.
            .selectExpr(
                "min(to_date(cast(date_key as string), 'yyyyMMdd')) as min_date",
                "max(to_date(cast(date_key as string), 'yyyyMMdd')) as max_date",
            )
            .first()
        )
        if existing and existing["min_date"]:
            start = min(start, existing["min_date"]) if start else existing["min_date"]
            end = max(end, existing["max_date"]) if end else existing["max_date"]

    return start or "2025-01-01", end or "2025-12-31"


def run_silver_pipeline(
    spark: SparkSession | None = None,
    bronze_dir: str = "warehouse/bronze",
    silver_dir: str = "warehouse/silver",
    quarantine_dir: str = "warehouse/quarantine",
) -> dict[str, Any]:
    """Reads Bronze tables, cleans new records, isolates invalid rows, and updates Silver tables.

    Order counts in the result (and the quality report) describe this run's new batch only.
    """
    spark_session = spark or get_spark_session(app_name="SilverPipeline")

    # Read Bronze tables
    bronze_customers_df = spark_session.read.format("delta").load(f"{bronze_dir}/customers")
    bronze_products_df = spark_session.read.format("delta").load(f"{bronze_dir}/products")
    bronze_orders_df = spark_session.read.format("delta").load(f"{bronze_dir}/orders")

    # Clean and upsert Customer and Product dimensions (small master data, always refreshed)
    clean_customers_df = clean_customers(bronze_customers_df)
    upsert_delta_table(spark_session, f"{silver_dir}/customers", clean_customers_df, merge_keys="customer_id")

    clean_products_df = clean_products(bronze_products_df)
    upsert_delta_table(spark_session, f"{silver_dir}/products", clean_products_df, merge_keys="product_id")

    summary: dict[str, Any] = {
        "customers_count": clean_customers_df.count(),
        "products_count": clean_products_df.count(),
    }

    # Only orders that have not been through Silver or quarantine before
    new_orders_df = select_unprocessed_orders(
        spark_session, bronze_orders_df, silver_dir, quarantine_dir
    ).cache()

    try:
        if new_orders_df.isEmpty():
            print("Silver: no new orders to process.")
            summary.update(
                new_orders_count=0,
                date_count=None,  # calendar not rebuilt on an idle run
                valid_orders_count=0,
                quarantined_orders_count=0,
                quality_report=None,
            )
            return summary

        # Deduplicate orders and parse dates
        deduplicated_orders_df = new_orders_df.dropDuplicates(["order_id"])
        orders_with_dates_df = parse_mixed_dates(deduplicated_orders_df)

        # Generate the calendar dimension for the date range seen so far
        start_date, end_date = calendar_bounds(spark_session, orders_with_dates_df, silver_dir)
        dense_calendar_df = generate_dense_calendar(spark_session, min_date=start_date, max_date=end_date)
        upsert_delta_table(spark_session, f"{silver_dir}/date", dense_calendar_df, merge_keys="date_key")

        # Calculate financial metrics and assign order UUIDs
        orders_with_financials_df = calculate_financial_metrics(orders_with_dates_df).withColumn(
            "order_uuid", to_uuid5(col("order_id"))
        )

        # Evaluate quality rules to split valid and quarantined records
        valid_orders_df, quarantine_orders_df = evaluate_data_quality(
            orders_with_financials_df, clean_customers_df, clean_products_df
        )
        valid_orders_df = valid_orders_df.cache()
        quarantine_orders_df = quarantine_orders_df.cache()

        try:
            valid_orders_count = valid_orders_df.count()
            quarantined_orders_count = quarantine_orders_df.count()
            total_orders_count = valid_orders_count + quarantined_orders_count
            # Taken before any write: the batch reads Silver and quarantine, which the writes below change.
            rejection_breakdown = count_rejections(quarantine_orders_df) if quarantined_orders_count > 0 else {}

            # Every write below is idempotent and ordered so that a crash at any point leaves each
            # order either in Silver, in quarantine, or in neither (and so picked up again next run).
            # 1) Quarantine rejected orders (merge: a retried order is updated, never duplicated).
            if quarantined_orders_count > 0:
                quarantine_projection_df = quarantine_orders_df.select(
                    "order_id",
                    "customer_id",
                    "customer_full_name",
                    "customer_city",
                    "product_id",
                    "product_name",
                    "product_category",
                    "unit_price",
                    "quantity",
                    "discount",
                    "order_date",
                    "source_file",
                    "reject_reason",
                    "quarantined_at",
                )
                upsert_quarantine(spark_session, f"{quarantine_dir}/orders", quarantine_projection_df)

            # 2) Retried orders that are now valid leave quarantine before they enter Silver.
            remove_from_quarantine(spark_session, f"{quarantine_dir}/orders", valid_orders_df.select("order_id"))

            # 3) Project and upsert clean Silver orders
            silver_orders_projection_df = valid_orders_df.select(
                "order_id",
                "order_uuid",
                "customer_id",
                "customer_uuid",
                "product_id",
                "product_uuid",
                "date_key",
                "order_ts",
                "quantity",
                "unit_price",
                "discount",
                "gross_revenue",
                "net_revenue",
                "ingest_ts",
            )

            upsert_delta_table(
                spark_session, f"{silver_dir}/orders", silver_orders_projection_df, merge_keys="order_id"
            )
            optimize_delta_table(spark_session, f"{silver_dir}/orders", zorder_cols=["customer_id", "order_ts"])

            # The report is written last, so a failed run does not leave a report for work it did not finish.
            quality_report_dict = generate_and_save_quality_report(
                total_count=total_orders_count,
                passed_count=valid_orders_count,
                quarantine_df=quarantine_orders_df,
                report_output_path=f"{quarantine_dir}/quality_report.json",
                rejection_breakdown=rejection_breakdown,
            )
        finally:
            valid_orders_df.unpersist()
            quarantine_orders_df.unpersist()

        summary.update(
            new_orders_count=total_orders_count,
            date_count=dense_calendar_df.count(),
            valid_orders_count=valid_orders_count,
            quarantined_orders_count=quarantined_orders_count,
            quality_report=quality_report_dict,
        )
        return summary
    finally:
        new_orders_df.unpersist()


if __name__ == "__main__":
    result = run_silver_pipeline()
    print("Silver Pipeline completed:", result)
