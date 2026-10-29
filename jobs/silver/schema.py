"""Explicit schemas for Silver layer Delta tables and Quarantine."""

from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, StructField, StructType, TimestampType

# Cleaned Customer Dimension Schema
SILVER_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_uuid", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("region", StringType(), False),
    StructField("country", StringType(), False),
])

# Cleaned Product Dimension Schema
SILVER_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_uuid", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("unit_price", DecimalType(10, 2), False),
])

# Dense Date Dimension Schema
SILVER_DATE_SCHEMA = StructType([
    StructField("date_key", IntegerType(), False),
    StructField("full_date", DateType(), False),
    StructField("cal_year", IntegerType(), False),
    StructField("cal_quarter", IntegerType(), False),
    StructField("cal_month", IntegerType(), False),
    StructField("cal_day", IntegerType(), False),
    StructField("day_of_week", StringType(), False),
])

# Cleaned Fact Orders Schema
SILVER_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_uuid", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("customer_uuid", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_uuid", StringType(), False),
    StructField("date_key", IntegerType(), False),
    StructField("order_ts", TimestampType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(10, 2), False),
    StructField("discount", DecimalType(10, 2), False),
    StructField("gross_revenue", DecimalType(14, 2), False),
    StructField("net_revenue", DecimalType(14, 2), False),
    StructField("ingest_ts", TimestampType(), True),
])

# Quarantine Schema for Corrupted Orders
QUARANTINE_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_full_name", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("discount", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("reject_reason", StringType(), False),
    StructField("quarantined_at", TimestampType(), False),
])
