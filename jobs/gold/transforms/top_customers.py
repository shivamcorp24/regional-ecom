"""Ranks customers by spend per region."""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, concat_ws, countDistinct, dense_rank, round as spark_round, sum as spark_sum
from pyspark.sql.types import DecimalType, IntegerType


def compute_top_customers_per_region(
    orders_df: DataFrame,
    customers_df: DataFrame,
    top_n: int | None = None,
) -> DataFrame:
    """Ranks customers within each region by total net spend."""
    customer_cols = customers_df.select(
        "customer_id",
        "customer_uuid",
        "first_name",
        "last_name",
        "city",
        "state",
        "region",
    )

    joined_df = orders_df.select("order_id", "customer_id", "net_revenue").join(
        customer_cols,
        on="customer_id",
        how="inner",
    )

    customer_spend_df = (
        joined_df.groupBy(
            "region",
            "customer_id",
            "customer_uuid",
            "first_name",
            "last_name",
            "city",
            "state",
        )
        .agg(
            countDistinct("order_id").cast(IntegerType()).alias("total_orders"),
            spark_round(spark_sum("net_revenue"), 2).cast(DecimalType(14, 2)).alias("total_spend"),
        )
        .withColumn(
            "avg_order_value",
            spark_round(col("total_spend") / col("total_orders"), 2).cast(DecimalType(14, 2)),
        )
        .withColumn("customer_name", concat_ws(" ", col("first_name"), col("last_name")))
    )

    # Tie-break on customer_uuid so ranking matches the Postgres query (ordered by UUID).
    region_window = Window.partitionBy("region").orderBy(col("total_spend").desc(), col("customer_uuid").asc())
    ranked_df = (
        customer_spend_df
        .withColumn("regional_rank", dense_rank().over(region_window).cast(IntegerType()))
        .select(
            "region",
            "regional_rank",
            "customer_id",
            "customer_uuid",
            "customer_name",
            "city",
            "state",
            "total_orders",
            "total_spend",
            "avg_order_value",
        )
        .orderBy("region", "regional_rank")
    )

    if top_n:
        ranked_df = ranked_df.filter(col("regional_rank") <= top_n)

    return ranked_df
