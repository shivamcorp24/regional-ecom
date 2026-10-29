"""Gold Mart 5: Product SKU performance, profitability, and discount analysis."""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, countDistinct, dense_rank, round as spark_round, sum as spark_sum, when
from pyspark.sql.types import DecimalType, IntegerType


def compute_product_performance(
    orders_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Calculates product volume, net revenue, discount rates, and category rankings."""
    order_subset = orders_df.select(
        "order_id",
        "product_id",
        "quantity",
        "gross_revenue",
        "discount",
        "net_revenue",
    )
    product_subset = products_df.select(
        "product_id",
        "product_uuid",
        "product_name",
        "category",
        "unit_price",
    )

    joined_df = order_subset.join(product_subset, on="product_id", how="inner")

    aggregated_df = (
        joined_df.groupBy(
            "product_id",
            "product_uuid",
            "product_name",
            "category",
            "unit_price",
        )
        .agg(
            countDistinct("order_id").cast(IntegerType()).alias("total_orders"),
            spark_sum("quantity").cast(IntegerType()).alias("total_units_sold"),
            spark_round(spark_sum("gross_revenue"), 2).cast(DecimalType(16, 2)).alias("gross_revenue"),
            spark_round(spark_sum("discount"), 2).cast(DecimalType(16, 2)).alias("total_discount"),
            spark_round(spark_sum("net_revenue"), 2).cast(DecimalType(16, 2)).alias("net_revenue"),
        )
        .withColumn(
            "discount_rate_pct",
            when(col("gross_revenue") > 0, spark_round((col("total_discount") / col("gross_revenue")) * 100, 2))
            .otherwise(0.00)
            .cast(DecimalType(5, 2)),
        )
    )

    category_window = Window.partitionBy("category").orderBy(col("net_revenue").desc(), col("total_units_sold").desc())
    overall_window = Window.orderBy(col("net_revenue").desc(), col("total_units_sold").desc())

    ranked_df = (
        aggregated_df
        .withColumn("category_rank", dense_rank().over(category_window).cast(IntegerType()))
        .withColumn("overall_rank", dense_rank().over(overall_window).cast(IntegerType()))
        .select(
            "product_id",
            "product_uuid",
            "product_name",
            "category",
            "unit_price",
            "total_orders",
            "total_units_sold",
            "gross_revenue",
            "total_discount",
            "net_revenue",
            "discount_rate_pct",
            "category_rank",
            "overall_rank",
        )
        .orderBy("category", "category_rank")
    )

    return ranked_df

