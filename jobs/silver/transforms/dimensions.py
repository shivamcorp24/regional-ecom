"""Customer and Product dimension cleaning with deterministic UUID5 generation."""

import uuid
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, udf
from pyspark.sql.types import DecimalType, StringType

# 1-line deterministic UUID5 generator
to_uuid5 = udf(lambda x: str(uuid.uuid5(uuid.NAMESPACE_DNS, str(x))) if x else None, StringType())


def clean_customers(bronze_customers_df: DataFrame) -> DataFrame:
    """Cleans, trims whitespace, deduplicates, and enriches customers with deterministic UUID5."""
    # 1. Trim whitespace on all string columns
    trimmed_df = (
        bronze_customers_df
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("phone", trim(col("phone")))
        .withColumn("city", trim(col("city")))
        .withColumn("state", trim(col("state")))
        .withColumn("region", trim(col("region")))
        .withColumn("country", trim(col("country")))
    )

    # 2. Filter records missing mandatory customer_id
    valid_customers_df = trimmed_df.filter(
        col("customer_id").isNotNull() & (col("customer_id") != "")
    )

    # 3. Deduplicate on primary key and generate deterministic UUID
    clean_df = (
        valid_customers_df
        .dropDuplicates(["customer_id"])
        .withColumn("customer_uuid", to_uuid5(col("customer_id")))
        .select(
            "customer_id",
            "customer_uuid",
            "first_name",
            "last_name",
            "phone",
            "city",
            "state",
            "region",
            "country",
        )
    )

    return clean_df


def clean_products(bronze_products_df: DataFrame) -> DataFrame:
    """Cleans, trims strings, casts unit_price, deduplicates, and enriches products with deterministic UUID5."""
    # 1. Trim strings and cast unit price to Decimal(10, 2)
    trimmed_df = (
        bronze_products_df
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", trim(col("category")))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
    )

    # 2. Filter out products missing product_id or with negative prices
    valid_products_df = trimmed_df.filter(
        col("product_id").isNotNull() & (col("product_id") != "") &
        col("unit_price").isNotNull() & (col("unit_price") >= 0)
    )

    # 3. Deduplicate on primary key and generate deterministic UUID
    clean_df = (
        valid_products_df
        .dropDuplicates(["product_id"])
        .withColumn("product_uuid", to_uuid5(col("product_id")))
        .select(
            "product_id",
            "product_uuid",
            "product_name",
            "category",
            "unit_price",
        )
    )

    return clean_df
