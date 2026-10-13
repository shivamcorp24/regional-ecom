"""ORM models for the regional-ecom star schema.

Importing this package registers every table on ``Base.metadata``. Alembic's
autogenerate and ``create_all`` need the full graph to be present, and nothing
else guarantees that the four modules have been imported.
"""

from database.models.base import Base
from database.models.Customer import Customer
from database.models.Date import DateDimension
from database.models.Orders import Order
from database.models.Products import Product
from database.models.staging import (
    stg_customers,
    stg_date,
    stg_orders,
    stg_products,
)

__all__ = [
    "Base",
    "Customer",
    "DateDimension",
    "Order",
    "Product",
    "stg_customers",
    "stg_date",
    "stg_orders",
    "stg_products",
]
