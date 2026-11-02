"""Data quality checks and quarantine routing for orders."""

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
    """Validates orders against quality rules and separates valid from quarantined records."""
    customer_lookup = customers_df.select(
        col("customer_id").alias("matched_customer_id"),
        col("customer_uuid"),
    )
    product_lookup = products_df.select(
        col("product_id").alias("matched_product_id"),
        col("product_uuid"),
    )

    joined_df = (
        orders_df
        .join(customer_lookup, orders_df["customer_id"] == customer_lookup["matched_customer_id"], "left")
        .join(product_lookup, orders_df["product_id"] == product_lookup["matched_product_id"], "left")
    )

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
    """Writes a summary of data quality results to a JSON file."""
    quarantined_count = total_count - passed_count
    pass_rate_pct = round((passed_count / total_count) * 100, 2) if total_count > 0 else 0.0

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

    os.makedirs(os.path.dirname(report_output_path), exist_ok=True)
    with open(report_output_path, "w", encoding="utf-8") as report_file:
        json.dump(report_data, report_file, indent=2)

    return report_data
