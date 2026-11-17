"""Repository for loading staging data into serving tables."""

from decimal import Decimal
import uuid
from sqlalchemy import delete, func, select, text
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

    def truncate_serving_tables(self) -> None:
        """Empties fact and dimension tables in one statement (transactional in PostgreSQL)."""
        self.session.execute(text("TRUNCATE TABLE fact_orders, dim_customers, dim_products, dim_date CASCADE"))

    def transfer_staging_to_serving(self) -> dict[str, int]:
        """Set-based bulk load: INSERT ... SELECT from staging, executed inside PostgreSQL.

        Dimensions are loaded before the fact table so foreign keys resolve. Casts mirror the
        ORM column types; NULL discounts default to 0.00.
        """
        statements = {
            "loaded_customers": """
                INSERT INTO dim_customers (customer_id, first_name, last_name, phone, city, state, region, country)
                SELECT customer_id::uuid, first_name, last_name, phone, city, state, region, country
                FROM stg_customers
            """,
            "loaded_products": """
                INSERT INTO dim_products (product_id, product_name, category, unit_price)
                SELECT product_id::uuid, product_name, category, unit_price
                FROM stg_products
            """,
            "loaded_dates": """
                INSERT INTO dim_date (date_key, full_date, cal_year, cal_quarter, cal_month, cal_day, day_of_week)
                SELECT date_key::integer, full_date, cal_year, cal_quarter, cal_month, cal_day, day_of_week
                FROM stg_date
            """,
            "loaded_orders": """
                INSERT INTO fact_orders (order_id, customer_id, product_id, date_key, order_ts, quantity,
                                         unit_price, discount, gross_revenue, net_revenue)
                SELECT order_id::uuid, customer_id::uuid, product_id::uuid, date_key::integer, order_ts, quantity,
                       unit_price, COALESCE(discount, 0.00), gross_revenue, net_revenue
                FROM stg_orders
            """,
        }
        return {key: self.session.execute(text(sql)).rowcount for key, sql in statements.items()}

    def transfer_staging_to_serving_orm(self) -> dict[str, int]:
        """ORM row-by-row transfer. Slower; kept for small loads and as a reference implementation."""
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
