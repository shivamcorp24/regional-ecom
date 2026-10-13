from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import CheckConstraint, ForeignKey, Index, Integer, Numeric, TIMESTAMP, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from database.models.base import Base


class Order(Base):
    """Order fact table (fact_orders) at order-line grain."""

    __tablename__ = "fact_orders"

    order_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    customer_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("dim_customers.customer_id", name="fk_fact_orders_customer_id"), nullable=False)
    product_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("dim_products.product_id", name="fk_fact_orders_product_id"), nullable=False)
    date_key: Mapped[int] = mapped_column(Integer, ForeignKey("dim_date.date_key", name="fk_fact_orders_date_key"), nullable=False)
    order_ts: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)

    # unit_price is kept on the fact so per-order overrides/corruptions are preserved
    # and gross_revenue remains reproducible from the fact row alone.
    unit_price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    discount: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False, server_default=text("0.00"))
    gross_revenue: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    net_revenue: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)

    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_fact_orders_quantity"),
        CheckConstraint("unit_price >= 0", name="ck_fact_orders_unit_price"),
        CheckConstraint("discount >= 0", name="ck_fact_orders_discount"),
        CheckConstraint("gross_revenue >= 0", name="ck_fact_orders_gross_revenue"),
        # net_revenue >= 0 ensures discount cannot exceed gross_revenue (net = gross - discount)
        CheckConstraint("net_revenue >= 0", name="ck_fact_orders_net_revenue_non_negative"),
        CheckConstraint("net_revenue <= gross_revenue", name="ck_fact_orders_net_revenue"),
        Index("ix_fact_orders_customer_id", "customer_id"),
        Index("ix_fact_orders_product_id", "product_id"),
        Index("ix_fact_orders_date_key", "date_key"),
    )