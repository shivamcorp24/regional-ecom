from typing import Annotated
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, StringConstraints

ShortText = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=50)
]
Phone = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=20)
]


class Customer(BaseModel):
    customer_id: UUID = Field(default_factory=uuid4)
    first_name: ShortText
    last_name: ShortText
    phone: Phone
    city: ShortText
    state: ShortText
    region: ShortText
    country: ShortText
