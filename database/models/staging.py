from sqlalchemy import Column, Date, Integer, Numeric, Table, Text, TIMESTAMP

from database.models.base import Base

# Unconstrained landing tables for Spark JDBC writes (no PKs, FKs, or check constraints).
stg_customers = Table(
    "stg_customers",
    Base.metadata,
    Column("customer_id", Text, nullable=True),
    Column("first_name", Text, nullable=True),
    Column("last_name", Text, nullable=True),
    Column("phone", Text, nullable=True),
    Column("city", Text, nullable=True),
    Column("state", Text, nullable=True),
    Column("region", Text, nullable=True),
    Column("country", Text, nullable=True),
)

stg_products = Table(
    "stg_products",
    Base.metadata,
    Column("product_id", Text, nullable=True),
    Column("product_name", Text, nullable=True),
    Column("category", Text, nullable=True),
    Column("unit_price", Numeric(10, 2), nullable=True),
)

stg_date = Table(
    "stg_date",
    Base.metadata,
    Column("date_key", Text, nullable=True),
    Column("full_date", Date, nullable=True),
    Column("cal_year", Integer, nullable=True),
    Column("cal_quarter", Integer, nullable=True),
    Column("cal_month", Integer, nullable=True),
    Column("cal_day", Integer, nullable=True),
    Column("day_of_week", Text, nullable=True),
)

stg_orders = Table(
    "stg_orders",
    Base.metadata,
    Column("order_id", Text, nullable=True),
    Column("customer_id", Text, nullable=True),
    Column("product_id", Text, nullable=True),
    Column("date_key", Text, nullable=True),
    Column("order_ts", TIMESTAMP, nullable=True),
    Column("quantity", Integer, nullable=True),
    Column("unit_price", Numeric(10, 2), nullable=True),
    Column("discount", Numeric(10, 2), nullable=True),
    Column("gross_revenue", Numeric(14, 2), nullable=True),
    Column("net_revenue", Numeric(14, 2), nullable=True),
)

__all__ = ["stg_customers", "stg_products", "stg_date", "stg_orders"]