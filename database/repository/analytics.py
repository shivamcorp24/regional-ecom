"""Analytical queries against PostgreSQL serving tables."""

from decimal import Decimal
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from database.models import Customer, DateDimension, Order, Product


class ServingAnalyticsRepository:
    """Queries serving tables for reconciliation checks."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_table_counts(self) -> dict[str, int]:
        """Returns row counts for serving tables."""
        return {
            "dim_customers": self.session.scalar(select(func.count()).select_from(Customer)) or 0,
            "dim_products": self.session.scalar(select(func.count()).select_from(Product)) or 0,
            "dim_date": self.session.scalar(select(func.count()).select_from(DateDimension)) or 0,
            "fact_orders": self.session.scalar(select(func.count()).select_from(Order)) or 0,
        }

    def get_financial_totals(self) -> dict[str, Decimal | int]:
        """Calculates total order count and revenue sums."""
        stmt = select(
            func.count(Order.order_id).label("total_orders"),
            func.sum(Order.quantity).label("total_quantity"),
            func.sum(Order.gross_revenue).label("total_gross_revenue"),
            func.sum(Order.discount).label("total_discount"),
            func.sum(Order.net_revenue).label("total_net_revenue"),
        )
        row = self.session.execute(stmt).mappings().one()
        return {
            "total_orders": int(row["total_orders"] or 0),
            "total_quantity": int(row["total_quantity"] or 0),
            "total_gross_revenue": Decimal(str(row["total_gross_revenue"] or "0.00")),
            "total_discount": Decimal(str(row["total_discount"] or "0.00")),
            "total_net_revenue": Decimal(str(row["total_net_revenue"] or "0.00")),
        }

    def get_monthly_revenue_summary(self) -> list[dict]:
        """Calculates monthly revenue metrics."""
        stmt = (
            select(
                DateDimension.cal_year,
                DateDimension.cal_month,
                func.count(Order.order_id).label("total_orders"),
                func.sum(Order.quantity).label("total_units_sold"),
                func.sum(Order.gross_revenue).label("gross_revenue"),
                func.sum(Order.discount).label("total_discount"),
                func.sum(Order.net_revenue).label("net_revenue"),
            )
            .join(DateDimension, Order.date_key == DateDimension.date_key)
            .group_by(DateDimension.cal_year, DateDimension.cal_month)
            .order_by(DateDimension.cal_year, DateDimension.cal_month)
        )
        rows = self.session.execute(stmt).mappings().all()
        return [
            {
                "cal_year": int(r["cal_year"]),
                "cal_month": int(r["cal_month"]),
                "total_orders": int(r["total_orders"]),
                "total_units_sold": int(r["total_units_sold"]),
                "gross_revenue": Decimal(str(r["gross_revenue"])),
                "total_discount": Decimal(str(r["total_discount"])),
                "net_revenue": Decimal(str(r["net_revenue"])),
            }
            for r in rows
        ]

    def get_category_revenue_summary(self) -> list[dict]:
        """Calculates category revenue totals by year."""
        stmt = (
            select(
                DateDimension.cal_year,
                Product.category,
                func.sum(Order.quantity).label("category_units_sold"),
                func.sum(Order.net_revenue).label("category_net_revenue"),
            )
            .join(Product, Order.product_id == Product.product_id)
            .join(DateDimension, Order.date_key == DateDimension.date_key)
            .group_by(DateDimension.cal_year, Product.category)
            .order_by(DateDimension.cal_year, func.sum(Order.net_revenue).desc())
        )
        rows = self.session.execute(stmt).mappings().all()
        return [
            {
                "cal_year": int(r["cal_year"]),
                "category": str(r["category"]),
                "category_units_sold": int(r["category_units_sold"]),
                "category_net_revenue": Decimal(str(r["category_net_revenue"])),
            }
            for r in rows
        ]

    def get_regional_top_spenders(self) -> list[dict]:
        """Finds top spenders per region."""
        regional_spend_subq = (
            select(
                Customer.region,
                Customer.customer_id,
                func.sum(Order.net_revenue).label("total_spend"),
                func.dense_rank().over(
                    partition_by=Customer.region,
                    order_by=[func.sum(Order.net_revenue).desc(), Customer.customer_id.asc()],
                ).label("regional_rank"),
            )
            .join(Customer, Order.customer_id == Customer.customer_id)
            .group_by(Customer.region, Customer.customer_id)
        ).subquery()

        stmt = (
            select(
                regional_spend_subq.c.region,
                regional_spend_subq.c.customer_id,
                regional_spend_subq.c.total_spend,
                regional_spend_subq.c.regional_rank,
            )
            .where(regional_spend_subq.c.regional_rank == 1)
            .order_by(regional_spend_subq.c.region)
        )
        rows = self.session.execute(stmt).mappings().all()
        return [
            {
                "region": str(r["region"]),
                "customer_id": str(r["customer_id"]),
                "total_spend": Decimal(str(r["total_spend"])),
                "regional_rank": int(r["regional_rank"]),
            }
            for r in rows
        ]
