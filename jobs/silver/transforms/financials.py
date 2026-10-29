"""Financial precision contract: deterministic monetary calculations using DecimalType(14, 2)."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, round as spark_round, when
from pyspark.sql.types import DecimalType, IntegerType


def calculate_financial_metrics(df: DataFrame) -> DataFrame:
    """Enforces the Financial Precision Contract on order metrics.

    Calculations:
      - quantity      : Cast to IntegerType
      - unit_price    : Cast to DecimalType(10, 2)
      - discount      : NULL or empty string coerced to 0.00, cast to DecimalType(10, 2)
      - gross_revenue : round(quantity * unit_price, 2) cast to DecimalType(14, 2)
      - net_revenue   : round(gross_revenue - discount, 2) cast to DecimalType(14, 2)

    Returns:
        DataFrame: With typed financial columns preserving exact cents precision.
    """
    # 1. Cast quantity and unit price
    quantity_expr = col("quantity").cast(IntegerType())
    unit_price_expr = col("unit_price").cast(DecimalType(10, 2))

    # 2. Coalesce discount to 0.00 when NULL or empty string
    discount_expr = coalesce(
        when(col("discount") == "", lit("0.00")).otherwise(col("discount")),
        lit("0.00"),
    ).cast(DecimalType(10, 2))

    # 3. Calculate gross revenue: quantity * unit_price
    gross_revenue_expr = spark_round(quantity_expr * unit_price_expr, 2).cast(DecimalType(14, 2))

    # 4. Attach intermediate financial columns
    with_gross_df = (
        df.withColumn("quantity", quantity_expr)
          .withColumn("unit_price", unit_price_expr)
          .withColumn("discount", discount_expr)
          .withColumn("gross_revenue", gross_revenue_expr)
    )

    # 5. Calculate net revenue: gross_revenue - discount
    net_revenue_expr = spark_round(col("gross_revenue") - col("discount"), 2).cast(DecimalType(14, 2))

    transformed_df = with_gross_df.withColumn("net_revenue", net_revenue_expr)

    return transformed_df
