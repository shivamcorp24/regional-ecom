"""Repository for loading staging data into serving tables."""

from decimal import Decimal
import uuid
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from database.models import Customer, DateDimension, Order, Product
from database.models.staging import stg_customers, stg_date, stg_orders, stg_products


class ServingRepository:
    """Handles database operations for serving tables."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def clean_serving_tables(self) -> None:
        """Deletes existing records from fact and dimension tables."""
        self.session.execute(delete(Order))
        self.session.execute(delete(Customer))
        self.session.execute(delete(Product))
        self.session.execute(delete(DateDimension))
        self.session.flush()

    def transfer_staging_to_serving(self) -> dict[str, int]:
        """Reads rows from staging tables and inserts them into serving tables."""
        cust_rows = self.session.execute(select(stg_customers)).mappings().all()
        customers = [
            Customer(
                customer_id=uuid.UUID(r["customer_id"]),
                first_name=r["first_name"],
                last_name=r["last_name"],
                phone=r["phone"],
                city=r["city"],
                state=r["state"],
                region=r["region"],
                country=r["country"],
            )
            for r in cust_rows
        ]
        self.session.add_all(customers)

        prod_rows = self.session.execute(select(stg_products)).mappings().all()
        products = [
            Product(
                product_id=uuid.UUID(r["product_id"]),
                product_name=r["product_name"],
                category=r["category"],
                unit_price=Decimal(str(r["unit_price"])),
            )
            for r in prod_rows
        ]
        self.session.add_all(products)

        date_rows = self.session.execute(select(stg_date)).mappings().all()
        dates = [
            DateDimension(
                date_key=int(r["date_key"]),
                full_date=r["full_date"],
                cal_year=int(r["cal_year"]),
                cal_quarter=int(r["cal_quarter"]),
                cal_month=int(r["cal_month"]),
                cal_day=int(r["cal_day"]),
                day_of_week=r["day_of_week"],
            )
            for r in date_rows
        ]
        self.session.add_all(dates)
        self.session.flush()

        order_rows = self.session.execute(select(stg_orders)).mappings().all()
        orders = [
            Order(
                order_id=uuid.UUID(r["order_id"]),
                customer_id=uuid.UUID(r["customer_id"]),
                product_id=uuid.UUID(r["product_id"]),
                date_key=int(r["date_key"]),
                order_ts=r["order_ts"],
                quantity=int(r["quantity"]),
                unit_price=Decimal(str(r["unit_price"])),
                discount=Decimal(str(r["discount"])) if r["discount"] is not None else Decimal("0.00"),
                gross_revenue=Decimal(str(r["gross_revenue"])),
                net_revenue=Decimal(str(r["net_revenue"])),
            )
            for r in order_rows
        ]
        self.session.add_all(orders)
        self.session.flush()

        return {
            "loaded_customers": len(customers),
            "loaded_products": len(products),
            "loaded_dates": len(dates),
            "loaded_orders": len(orders),
        }

    def get_serving_counts(self) -> dict[str, int]:
        """Returns row counts for serving tables."""
        return {
            "dim_customers": self.session.scalar(select(func.count()).select_from(Customer)) or 0,
            "dim_products": self.session.scalar(select(func.count()).select_from(Product)) or 0,
            "dim_date": self.session.scalar(select(func.count()).select_from(DateDimension)) or 0,
            "fact_orders": self.session.scalar(select(func.count()).select_from(Order)) or 0,
        }
