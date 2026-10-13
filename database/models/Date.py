from datetime import date

from sqlalchemy import CheckConstraint, Date, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from database.models.base import Base


class DateDimension(Base):
    """Date dimension (dim_date), one row per calendar day."""

    __tablename__ = "dim_date"

    # autoincrement=False prevents SQLAlchemy from rendering the INTEGER
    # primary key as a SERIAL sequence, avoiding conflicts with ETL-supplied YYYYMMDD keys.
    date_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    full_date: Mapped[date] = mapped_column(Date, unique=True, nullable=False)
    cal_year: Mapped[int] = mapped_column(Integer, nullable=False)
    cal_quarter: Mapped[int] = mapped_column(Integer, nullable=False)
    cal_month: Mapped[int] = mapped_column(Integer, nullable=False)
    cal_day: Mapped[int] = mapped_column(Integer, nullable=False)
    day_of_week: Mapped[str] = mapped_column(String(15), nullable=False)

    __table_args__ = (
        CheckConstraint("cal_quarter BETWEEN 1 AND 4", name="ck_dim_date_quarter"),
        CheckConstraint("cal_month BETWEEN 1 AND 12", name="ck_dim_date_month"),
        CheckConstraint("cal_day BETWEEN 1 AND 31", name="ck_dim_date_day"),
        CheckConstraint("cal_year BETWEEN 1900 AND 2100", name="ck_dim_date_year"),
        CheckConstraint("btrim(day_of_week) <> ''", name="ck_dim_date_day_of_week"),
    )