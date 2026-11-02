"""Computes customer RFM scores and segments."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    countDistinct,
    datediff,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
    to_date,
    when,
)
from pyspark.sql.types import DecimalType, IntegerType


def compute_customer_rfm(
    orders_df: DataFrame,
    customers_df: DataFrame,
    reference_date=None,
) -> DataFrame:
    """Calculates recency, frequency, and monetary scores per customer."""
    if reference_date is None:
        max_row = orders_df.selectExpr("max(to_date(order_ts)) as max_d").first()
        ref_date = max_row["max_d"] if max_row and max_row["max_d"] else "2025-12-31"
    else:
        ref_date = str(reference_date)

    order_subset = orders_df.select("order_id", "customer_id", "order_ts", "net_revenue")
    customer_subset = customers_df.select(
        "customer_id",
        "customer_uuid",
        "first_name",
        "last_name",
        "city",
        "state",
        "region",
    )

    joined_df = order_subset.join(customer_subset, on="customer_id", how="inner")

    customer_metrics_df = (
        joined_df.groupBy(
            "customer_id",
            "customer_uuid",
            "first_name",
            "last_name",
            "city",
            "state",
            "region",
        )
        .agg(
            spark_min("order_ts").alias("first_order_ts"),
            spark_max("order_ts").alias("last_order_ts"),
            countDistinct("order_id").cast(IntegerType()).alias("frequency"),
            spark_round(spark_sum("net_revenue"), 2).cast(DecimalType(14, 2)).alias("monetary_value"),
        )
        .withColumn("customer_name", concat_ws(" ", col("first_name"), col("last_name")))
        .withColumn(
            "recency_days",
            datediff(lit(ref_date), to_date(col("last_order_ts"))).cast(IntegerType()),
        )
        .withColumn(
            "avg_order_value",
            spark_round(col("monetary_value") / col("frequency"), 2).cast(DecimalType(14, 2)),
        )
    )

    r_score_expr = (
        when(col("recency_days") <= 30, lit(4))
        .when(col("recency_days") <= 90, lit(3))
        .when(col("recency_days") <= 180, lit(2))
        .otherwise(lit(1))
    ).cast(IntegerType())

    f_score_expr = (
        when(col("frequency") >= 10, lit(4))
        .when(col("frequency") >= 5, lit(3))
        .when(col("frequency") >= 2, lit(2))
        .otherwise(lit(1))
    ).cast(IntegerType())

    m_score_expr = (
        when(col("monetary_value") >= 1000, lit(4))
        .when(col("monetary_value") >= 500, lit(3))
        .when(col("monetary_value") >= 200, lit(2))
        .otherwise(lit(1))
    ).cast(IntegerType())

    rfm_segment_expr = (
        when((col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") >= 3), lit("Champions"))
        .when((col("f_score") >= 3) & (col("m_score") >= 2), lit("Loyal Customers"))
        .when((col("r_score") <= 2) & (col("f_score") >= 3), lit("At Risk"))
        .when((col("r_score") >= 3) & (col("f_score") == 1), lit("New Customers"))
        .when((col("r_score") <= 1) & (col("f_score") <= 2), lit("Hibernating / Lost"))
        .otherwise(lit("Potential Loyalists"))
    )

    return (
        customer_metrics_df
        .withColumn("r_score", r_score_expr)
        .withColumn("f_score", f_score_expr)
        .withColumn("m_score", m_score_expr)
        .withColumn("rfm_segment", rfm_segment_expr)
        .select(
            "customer_id",
            "customer_uuid",
            "customer_name",
            "city",
            "state",
            "region",
            "first_order_ts",
            "last_order_ts",
            "recency_days",
            "frequency",
            "monetary_value",
            "avg_order_value",
            "r_score",
            "f_score",
            "m_score",
            "rfm_segment",
        )
        .orderBy(col("monetary_value").desc())
    )
