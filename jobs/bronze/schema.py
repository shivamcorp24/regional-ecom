"""Bronze layer Ingestion: loads raw csv into append-only Delta tables with ingestion metadata"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType, StringType

# Raw Orders Schema
RAW_ORDERS_SCHEMA = StructType([
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
])

# Customers Schema
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("region", StringType(), True),
    StructField("country", StringType(), True),
])

# Products Schema
PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", StringType(), True),
])
