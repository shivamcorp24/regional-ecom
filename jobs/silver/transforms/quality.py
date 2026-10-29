"""Data quality validation rules and diagnostic quarantine routing engine."""

import json
import os
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, when


def evaluate_data_quality(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Validates order records and routes flawed records to quarantine with diagnostic reasons.

    Validation Rules:
      1. order_id must not be NULL or empty string
      2. order_ts must be successfully parsed (not NULL)
      3. quantity must be > 0
      4. unit_price must be >= 0
      5. discount must be >= 0
      6. net_revenue must be >= 0 (discount cannot exceed gross revenue)
      7. customer_id must exist in clean customers dimension
      8. product_id must exist in clean products dimension

    Returns:
        tuple[DataFrame, DataFrame]: (valid_orders_df, quarantine_orders_df)
    """
    # 1. Project customer and product dimension keys for referential integrity lookup
    customer_lookup = customers_df.select(
        col("customer_id").alias("matched_customer_id"),
        col("customer_uuid"),
    )
    product_lookup = products_df.select(
        col("product_id").alias("matched_product_id"),
        col("product_uuid"),
    )

    # 2. Left join orders with dimension lookups
    joined_df = (
        orders_df
        .join(customer_lookup, orders_df["customer_id"] == customer_lookup["matched_customer_id"], "left")
        .join(product_lookup, orders_df["product_id"] == product_lookup["matched_product_id"], "left")
    )

    # 3. Prioritized diagnostic reject reason evaluation
    reject_reason_expr = (
        when(col("order_id").isNull() | (col("order_id") == ""), lit("MISSING_ORDER_ID"))
        .when(col("order_ts").isNull(), lit("INVALID_DATE_FORMAT"))
        .when(col("quantity").isNull() | (col("quantity") <= 0), lit("INVALID_QUANTITY"))
        .when(col("unit_price").isNull() | (col("unit_price") < 0), lit("INVALID_PRICE"))
        .when(col("discount").isNull() | (col("discount") < 0), lit("NEGATIVE_DISCOUNT"))
        .when(col("net_revenue").isNull() | (col("net_revenue") < 0), lit("DISCOUNT_EXCEEDS_GROSS"))
        .when(col("matched_customer_id").isNull(), lit("UNKNOWN_CUSTOMER_ID"))
        .when(col("matched_product_id").isNull(), lit("UNKNOWN_PRODUCT_ID"))
        .otherwise(None)
    )

    evaluated_df = joined_df.withColumn("reject_reason", reject_reason_expr)

    # 4. Filter into valid and quarantine DataFrames
    valid_orders_df = (
        evaluated_df
        .filter(col("reject_reason").isNull())
        .drop("matched_customer_id", "matched_product_id", "reject_reason")
    )

    quarantine_orders_df = (
        evaluated_df
        .filter(col("reject_reason").isNotNull())
        .withColumn("quarantined_at", current_timestamp())
        .drop("matched_customer_id", "matched_product_id", "customer_uuid", "product_uuid")
    )

    return valid_orders_df, quarantine_orders_df


def generate_and_save_quality_report(
    total_count: int,
    passed_count: int,
    quarantine_df: DataFrame,
    report_output_path: str,
) -> dict:
    """Calculates summary data quality metrics and writes quality_report.json to disk.

    Returns:
        dict: Summary report containing total, passed, quarantined, and rejection breakdown.
    """
    quarantined_count = total_count - passed_count
    pass_rate_pct = round((passed_count / total_count) * 100, 2) if total_count > 0 else 0.0

    # Aggregate rejection reasons from quarantine DataFrame
    rejection_breakdown = {}
    if quarantined_count > 0:
        aggregated_rows = (
            quarantine_df.groupBy("reject_reason")
            .count()
            .collect()
        )
        rejection_breakdown = {row["reject_reason"]: row["count"] for row in aggregated_rows}

    report_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_records": total_count,
        "passed_records": passed_count,
        "quarantined_records": quarantined_count,
        "pass_rate_pct": pass_rate_pct,
        "rejection_breakdown": rejection_breakdown,
    }

    # Ensure parent directory exists before writing
    os.makedirs(os.path.dirname(report_output_path), exist_ok=True)
    with open(report_output_path, "w", encoding="utf-8") as report_file:
        json.dump(report_data, report_file, indent=2)

    return report_data
