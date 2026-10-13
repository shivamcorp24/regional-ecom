from datetime import date

from pydantic import BaseModel, Field


class DateDimension(BaseModel):
    date_key: int = Field(ge=19000101, le=21001231)
    full_date: date
    cal_year: int = Field(ge=1900, le=2100)
    cal_quarter: int = Field(ge=1, le=4)
    cal_month: int = Field(ge=1, le=12)
    cal_day: int = Field(ge=1, le=31)
    day_of_week: str = Field(min_length=1, max_length=15)
