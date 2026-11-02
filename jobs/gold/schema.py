"""Schemas for Gold layer tables."""

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Monthly revenue mart
GOLD_MONTHLY_REVENUE_SCHEMA = StructType([
    StructField("cal_year", IntegerType(), False),
    StructField("cal_month", IntegerType(), False),
    StructField("total_orders", IntegerType(), False),
    StructField("total_units_sold", IntegerType(), False),
    StructField("gross_revenue", DecimalType(16, 2), False),
    StructField("total_discount", DecimalType(16, 2), False),
    StructField("net_revenue", DecimalType(16, 2), False),
    StructField("avg_order_value", DecimalType(14, 2), False),
])

# Top customers per region mart
GOLD_TOP_CUSTOMERS_SCHEMA = StructType([
    StructField("region", StringType(), False),
    StructField("regional_rank", IntegerType(), False),
    StructField("customer_id", StringType(), False),
    StructField("customer_uuid", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("total_orders", IntegerType(), False),
    StructField("total_spend", DecimalType(14, 2), False),
    StructField("avg_order_value", DecimalType(14, 2), False),
])

# Category share mart
GOLD_CATEGORY_SHARE_SCHEMA = StructType([
    StructField("cal_year", IntegerType(), False),
    StructField("category", StringType(), False),
    StructField("category_units_sold", IntegerType(), False),
    StructField("category_net_revenue", DecimalType(16, 2), False),
    StructField("annual_net_revenue", DecimalType(16, 2), False),
    StructField("revenue_share_pct", DecimalType(5, 2), False),
])

# Customer RFM mart
GOLD_CUSTOMER_RFM_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_uuid", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("region", StringType(), False),
    StructField("first_order_ts", TimestampType(), False),
    StructField("last_order_ts", TimestampType(), False),
    StructField("recency_days", IntegerType(), False),
    StructField("frequency", IntegerType(), False),
    StructField("monetary_value", DecimalType(14, 2), False),
    StructField("avg_order_value", DecimalType(14, 2), False),
    StructField("r_score", IntegerType(), False),
    StructField("f_score", IntegerType(), False),
    StructField("m_score", IntegerType(), False),
    StructField("rfm_segment", StringType(), False),
])

# Product performance mart
GOLD_PRODUCT_PERFORMANCE_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_uuid", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("unit_price", DecimalType(10, 2), False),
    StructField("total_orders", IntegerType(), False),
    StructField("total_units_sold", IntegerType(), False),
    StructField("gross_revenue", DecimalType(16, 2), False),
    StructField("total_discount", DecimalType(16, 2), False),
    StructField("net_revenue", DecimalType(16, 2), False),
    StructField("discount_rate_pct", DecimalType(5, 2), False),
    StructField("category_rank", IntegerType(), False),
    StructField("overall_rank", IntegerType(), False),
])
