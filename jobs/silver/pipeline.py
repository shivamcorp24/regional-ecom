"""Silver Layer Pipeline: Cleans dimensions, generates dense calendar, validates quality, and upserts."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from jobs.silver.transforms.dates import generate_dense_calendar, parse_mixed_dates
from jobs.silver.transforms.dimensions import clean_customers, clean_products, to_uuid5
from jobs.silver.transforms.financials import calculate_financial_metrics
from jobs.silver.transforms.quality import evaluate_data_quality, generate_and_save_quality_report
from jobs.silver.transforms.upsert import append_delta_table, optimize_delta_table, upsert_delta_table
from src.spark.session import get_spark_session


def run_silver_pipeline(
    spark: SparkSession | None = None,
    bronze_dir: str = "warehouse/bronze",
    silver_dir: str = "warehouse/silver",
    quarantine_dir: str = "warehouse/quarantine",
) -> dict:
    """Executes the complete Silver Medallion pipeline stage.

    Steps:
      1. Load Bronze Delta tables (customers, products, orders).
      2. Clean customer and product dimensions with deterministic UUID5; upsert to Silver.
      3. Deduplicate raw orders on primary order_id.
      4. Parse mixed date formats via shape-based regex routing.
      5. Generate a dense, gap-free calendar dimension; upsert to Silver.
      6. Enforce financial precision contract (gross & net revenue) and assign order_uuid via uuid5.
      7. Validate records against quality rules; route invalid rows to quarantine Delta table.
      8. Generate quality_report.json summary metrics.
      9. Project clean orders star schema and upsert into Silver Delta table.
      10. Execute OPTIMIZE ZORDER BY on silver orders.

    Returns:
        dict: Execution summary with record counts and data quality report.
    """
    spark_session = spark or get_spark_session(app_name="SilverPipeline")

    # Step 1: Load raw Bronze Delta tables
    bronze_customers_df = spark_session.read.format("delta").load(f"{bronze_dir}/customers")
    bronze_products_df = spark_session.read.format("delta").load(f"{bronze_dir}/products")
    bronze_orders_df = spark_session.read.format("delta").load(f"{bronze_dir}/orders")

    # Step 2: Clean Customer and Product Dimensions, then upsert
    clean_customers_df = clean_customers(bronze_customers_df)
    upsert_delta_table(spark_session, f"{silver_dir}/customers", clean_customers_df, merge_keys="customer_id")

    clean_products_df = clean_products(bronze_products_df)
    upsert_delta_table(spark_session, f"{silver_dir}/products", clean_products_df, merge_keys="product_id")

    # Step 3: Deduplicate Bronze Orders on order_id
    deduplicated_orders_df = bronze_orders_df.dropDuplicates(["order_id"])

    # Step 4: Parse mixed date formats
    orders_with_dates_df = parse_mixed_dates(deduplicated_orders_df)

    # Step 5: Derive date boundaries and generate dense calendar dimension
    date_bounds = (
        orders_with_dates_df.filter(col("order_ts").isNotNull())
        .selectExpr("min(to_date(order_ts)) as min_date", "max(to_date(order_ts)) as max_date")
        .first()
    )
    start_date = date_bounds["min_date"] if date_bounds and date_bounds["min_date"] else "2025-01-01"
    end_date = date_bounds["max_date"] if date_bounds and date_bounds["max_date"] else "2025-12-31"

    dense_calendar_df = generate_dense_calendar(spark_session, min_date=start_date, max_date=end_date)
    upsert_delta_table(spark_session, f"{silver_dir}/date", dense_calendar_df, merge_keys="date_key")

    # Step 6: Calculate financial metrics and add deterministic order_uuid via standard uuid5
    orders_with_financials_df = calculate_financial_metrics(orders_with_dates_df).withColumn(
        "order_uuid", to_uuid5(col("order_id"))
    )

    # Step 7: Evaluate data quality rules and split valid vs quarantine records
    valid_orders_df, quarantine_orders_df = evaluate_data_quality(
        orders_with_financials_df, clean_customers_df, clean_products_df
    )

    total_orders_count = deduplicated_orders_df.count()
    valid_orders_count = valid_orders_df.count()
    quarantined_orders_count = quarantine_orders_df.count()

    # Step 8: Persist quarantine records and generate quality report JSON
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
        append_delta_table(spark_session, f"{quarantine_dir}/orders", quarantine_projection_df)

    quality_report_dict = generate_and_save_quality_report(
        total_count=total_orders_count,
        passed_count=valid_orders_count,
        quarantine_df=quarantine_orders_df,
        report_output_path=f"{quarantine_dir}/quality_report.json",
    )

    # Step 9: Project clean Silver orders star schema
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

    # Step 10: Upsert clean orders into Silver Delta table
    upsert_delta_table(spark_session, f"{silver_dir}/orders", silver_orders_projection_df, merge_keys="order_id")

    # Step 11: Compaction and multi-dimensional Z-ORDER indexing
    optimize_delta_table(spark_session, f"{silver_dir}/orders", zorder_cols=["customer_id", "order_ts"])

    pipeline_summary = {
        "customers_count": clean_customers_df.count(),
        "products_count": clean_products_df.count(),
        "date_count": dense_calendar_df.count(),
        "valid_orders_count": valid_orders_count,
        "quarantined_orders_count": quarantined_orders_count,
        "quality_report": quality_report_dict,
    }

    return pipeline_summary


if __name__ == "__main__":
    summary = run_silver_pipeline()
    print("Silver Pipeline Completed Successfully:", summary)
