from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator


class Order(BaseModel):
    """A cleaned, warehouse-ready ``fact_orders`` row.

    This is *not* the raw CSV row -- it is the post-transform shape, so
    ``gross_revenue``/``net_revenue`` are derived rather than Optional. The
    raw ingest DTO belongs to the extraction stage.
    """

    order_id: UUID = Field(default_factory=uuid4)
    customer_id: UUID
    product_id: UUID
    date_key: int
    order_ts: datetime
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(ge=Decimal("0.00"), max_digits=10, decimal_places=2)
    discount: Decimal = Field(ge=Decimal("0.00"), max_digits=10, decimal_places=2)
    gross_revenue: Decimal = Field(ge=Decimal("0.00"), max_digits=14, decimal_places=2)
    net_revenue: Decimal = Field(
        ge=Decimal("0.00"), max_digits=14, decimal_places=2
    )

    @model_validator(mode="after")
    def _reconcile_revenue(self) -> "Order":
        expected_gross = (self.unit_price * self.quantity).quantize(Decimal("0.01"))
        if self.gross_revenue != expected_gross:
            raise ValueError(
                f"gross_revenue {self.gross_revenue} != quantity * unit_price "
                f"({expected_gross})"
            )
        expected_net = (self.gross_revenue - self.discount).quantize(Decimal("0.01"))
        if self.net_revenue != expected_net:
            raise ValueError(
                f"net_revenue {self.net_revenue} != gross_revenue - discount "
                f"({expected_net})"
            )
        return self
