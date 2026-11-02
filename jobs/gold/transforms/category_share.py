"""Computes annual category revenue share."""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, round as spark_round, sum as spark_sum
from pyspark.sql.types import DecimalType, IntegerType


def compute_category_share(
    orders_df: DataFrame,
    products_df: DataFrame,
    date_df: DataFrame,
) -> DataFrame:
    """Calculates category revenue and percentage share per year."""
    order_subset = orders_df.select("product_id", "date_key", "quantity", "net_revenue")
    product_subset = products_df.select("product_id", "category")
    date_subset = date_df.select("date_key", "cal_year")

    joined_df = (
        order_subset
        .join(product_subset, on="product_id", how="inner")
        .join(date_subset, on="date_key", how="inner")
    )

    category_summary_df = (
        joined_df.groupBy("cal_year", "category")
        .agg(
            spark_sum("quantity").cast(IntegerType()).alias("category_units_sold"),
            spark_round(spark_sum("net_revenue"), 2).cast(DecimalType(16, 2)).alias("category_net_revenue"),
        )
    )

    annual_window = Window.partitionBy("cal_year")
    return (
        category_summary_df
        .withColumn(
            "annual_net_revenue",
            spark_sum("category_net_revenue").over(annual_window).cast(DecimalType(16, 2)),
        )
        .withColumn(
            "revenue_share_pct",
            spark_round((col("category_net_revenue") / col("annual_net_revenue")) * 100, 2).cast(DecimalType(5, 2)),
        )
        .select(
            "cal_year",
            "category",
            "category_units_sold",
            "category_net_revenue",
            "annual_net_revenue",
            "revenue_share_pct",
        )
        .orderBy("cal_year", col("category_net_revenue").desc())
    )
