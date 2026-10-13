from decimal import Decimal
from uuid import uuid4

from sqlalchemy import CheckConstraint, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from database.models.base import Base


class Product(Base):
    """Product dimension (dim_products)."""

    __tablename__ = "dim_products"

    product_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    product_name: Mapped[str] = mapped_column(String(100), nullable=False)
    category: Mapped[str] = mapped_column(String(50), nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)

    __table_args__ = (
        CheckConstraint("unit_price >= 0", name="ck_dim_products_unit_price"),
        CheckConstraint("btrim(product_name) <> ''", name="ck_dim_products_product_name"),
        CheckConstraint("btrim(category) <> ''", name="ck_dim_products_category"),
    )
