"""Calculates order revenue metrics with fixed-point decimal precision."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, round as spark_round, when
from pyspark.sql.types import DecimalType, IntegerType


def calculate_financial_metrics(df: DataFrame) -> DataFrame:
    """Calculates gross revenue and net revenue using decimal casts."""
    quantity_expr = col("quantity").cast(IntegerType())
    unit_price_expr = col("unit_price").cast(DecimalType(10, 2))

    discount_expr = coalesce(
        when(col("discount") == "", lit("0.00")).otherwise(col("discount")),
        lit("0.00"),
    ).cast(DecimalType(10, 2))

    gross_revenue_expr = spark_round(quantity_expr * unit_price_expr, 2).cast(DecimalType(14, 2))

    with_gross_df = (
        df.withColumn("quantity", quantity_expr)
          .withColumn("unit_price", unit_price_expr)
          .withColumn("discount", discount_expr)
          .withColumn("gross_revenue", gross_revenue_expr)
    )

    net_revenue_expr = spark_round(col("gross_revenue") - col("discount"), 2).cast(DecimalType(14, 2))

    return with_gross_df.withColumn("net_revenue", net_revenue_expr)
