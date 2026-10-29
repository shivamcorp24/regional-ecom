"""Gold Mart 1: Monthly revenue, order volume, and average order value."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, countDistinct, round as spark_round, sum as spark_sum
from pyspark.sql.types import DecimalType, IntegerType


def compute_monthly_revenue(orders_df: DataFrame, date_df: DataFrame) -> DataFrame:
    """Aggregates financial performance metrics by calendar year and month."""
    joined_df = orders_df.join(
        date_df.select("date_key", "cal_year", "cal_month"),
        on="date_key",
        how="inner",
    )

    monthly_df = (
        joined_df.groupBy("cal_year", "cal_month")
        .agg(
            countDistinct("order_id").cast(IntegerType()).alias("total_orders"),
            spark_sum("quantity").cast(IntegerType()).alias("total_units_sold"),
            spark_round(spark_sum("gross_revenue"), 2).cast(DecimalType(16, 2)).alias("gross_revenue"),
            spark_round(spark_sum("discount"), 2).cast(DecimalType(16, 2)).alias("total_discount"),
            spark_round(spark_sum("net_revenue"), 2).cast(DecimalType(16, 2)).alias("net_revenue"),
        )
        .withColumn(
            "avg_order_value",
            spark_round(col("net_revenue") / col("total_orders"), 2).cast(DecimalType(14, 2)),
        )
        .select(
            "cal_year",
            "cal_month",
            "total_orders",
            "total_units_sold",
            "gross_revenue",
            "total_discount",
            "net_revenue",
            "avg_order_value",
        )
        .orderBy("cal_year", "cal_month")
    )

    return monthly_df

