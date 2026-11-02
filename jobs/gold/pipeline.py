"""Gold layer pipeline: builds aggregated reporting tables."""

from typing import Any
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from jobs.gold.transforms.category_share import compute_category_share
from jobs.gold.transforms.customer_rfm import compute_customer_rfm
from jobs.gold.transforms.monthly_revenue import compute_monthly_revenue
from jobs.gold.transforms.product_performance import compute_product_performance
from jobs.gold.transforms.top_customers import compute_top_customers_per_region
from src.spark.session import get_spark_session


def save_gold_table(
    spark: SparkSession,
    df: DataFrame,
    target_path: str,
    partition_cols: list[str] | None = None,
) -> None:
    """Writes a DataFrame to Delta format and runs file compaction."""
    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(target_path)

    if DeltaTable.isDeltaTable(spark, target_path):
        DeltaTable.forPath(spark, target_path).optimize().executeCompaction()


def run_gold_pipeline(
    spark: SparkSession | None = None,
    silver_dir: str = "warehouse/silver",
    gold_dir: str = "warehouse/gold",
) -> dict[str, int]:
    """Computes Gold aggregation tables and saves them to storage."""
    spark_session = spark or get_spark_session(app_name="GoldPipeline")

    # Read Silver tables
    orders_df = spark_session.read.format("delta").load(f"{silver_dir}/orders")
    customers_df = spark_session.read.format("delta").load(f"{silver_dir}/customers")
    products_df = spark_session.read.format("delta").load(f"{silver_dir}/products")
    date_df = spark_session.read.format("delta").load(f"{silver_dir}/date")

    # Compute Gold tables
    monthly_rev_df = compute_monthly_revenue(orders_df, date_df)
    top_customers_df = compute_top_customers_per_region(orders_df, customers_df)
    category_share_df = compute_category_share(orders_df, products_df, date_df)
    customer_rfm_df = compute_customer_rfm(orders_df, customers_df)
    product_perf_df = compute_product_performance(orders_df, products_df)

    # Save with partitioning and compaction
    save_gold_table(spark_session, monthly_rev_df, f"{gold_dir}/monthly_revenue", partition_cols=["cal_year"])
    save_gold_table(spark_session, top_customers_df, f"{gold_dir}/top_customers_per_region", partition_cols=["region"])
    save_gold_table(spark_session, category_share_df, f"{gold_dir}/category_share", partition_cols=["cal_year"])
    save_gold_table(spark_session, customer_rfm_df, f"{gold_dir}/customer_rfm_summary", partition_cols=["rfm_segment"])
    save_gold_table(spark_session, product_perf_df, f"{gold_dir}/product_performance", partition_cols=["category"])

    return {
        "monthly_revenue_rows": monthly_rev_df.count(),
        "top_customers_rows": top_customers_df.count(),
        "category_share_rows": category_share_df.count(),
        "customer_rfm_rows": customer_rfm_df.count(),
        "product_performance_rows": product_perf_df.count(),
    }


if __name__ == "__main__":
    summary = run_gold_pipeline()
    print("Gold Pipeline completed:", summary)
