from uuid import uuid4

from sqlalchemy import CheckConstraint, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from database.models.base import Base


class Customer(Base):
    """Customer dimension (dim_customers)."""

    __tablename__ = "dim_customers"

    customer_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    first_name: Mapped[str] = mapped_column(String(50), nullable=False)
    last_name: Mapped[str] = mapped_column(String(50), nullable=False)
    phone: Mapped[str] = mapped_column(String(20), nullable=False)
    city: Mapped[str] = mapped_column(String(50), nullable=False)
    state: Mapped[str] = mapped_column(String(50), nullable=False)
    region: Mapped[str] = mapped_column(String(50), nullable=False)
    country: Mapped[str] = mapped_column(String(50), nullable=False)

    __table_args__ = (
        CheckConstraint("btrim(first_name) <> ''", name="ck_dim_customers_first_name"),
        CheckConstraint("btrim(last_name) <> ''", name="ck_dim_customers_last_name"),
        CheckConstraint("btrim(city) <> ''", name="ck_dim_customers_city"),
        CheckConstraint("btrim(region) <> ''", name="ck_dim_customers_region"),
        CheckConstraint("btrim(country) <> ''", name="ck_dim_customers_country"),
    )
