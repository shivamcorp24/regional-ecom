"""Customer and product dimension cleaning and ID generation."""

import uuid
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, udf
from pyspark.sql.types import DecimalType, StringType


def _generate_deterministic_uuid(val: str | None) -> str | None:
    """Generates a UUID5 string from an input key value."""
    if not val:
        return None
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(val)))


to_uuid5 = udf(_generate_deterministic_uuid, StringType())


def clean_customers(bronze_customers_df: DataFrame) -> DataFrame:
    """Cleans whitespace, removes missing IDs, and generates UUIDs for customers."""
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

    valid_customers_df = trimmed_df.filter(
        col("customer_id").isNotNull() & (col("customer_id") != "")
    )

    return (
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


def clean_products(bronze_products_df: DataFrame) -> DataFrame:
    """Cleans fields, filters invalid rows, and generates UUIDs for products."""
    trimmed_df = (
        bronze_products_df
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("product_name", trim(col("product_name")))
        .withColumn("category", trim(col("category")))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(10, 2)))
    )

    valid_products_df = trimmed_df.filter(
        col("product_id").isNotNull() & (col("product_id") != "") &
        col("unit_price").isNotNull() & (col("unit_price") >= 0)
    )

    return (
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
