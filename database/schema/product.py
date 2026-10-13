from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Product(BaseModel):
    product_id: UUID = Field(default_factory=uuid4)
    product_name: str = Field(min_length=1, max_length=100)
    category: str = Field(min_length=1, max_length=50)
    unit_price: Decimal = Field(ge=Decimal("0.00"), max_digits=10, decimal_places=2)
