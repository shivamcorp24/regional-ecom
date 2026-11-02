"""Reconciliation checks between Delta Lake and PostgreSQL tables."""

from decimal import Decimal
import json
import os
from datetime import datetime, timezone
from typing import Any
from pyspark.sql import SparkSession

from core.database import get_db
from database.repository.analytics import ServingAnalyticsRepository
from src.spark.session import get_spark_session


class ReconciliationError(Exception):
    """Raised when reconciliation checks fail."""
    pass


def run_reconciliation(
    spark: SparkSession | None = None,
    silver_dir: str = "warehouse/silver",
    gold_dir: str = "warehouse/gold",
    report_output_path: str = "warehouse/reconciliation_report.json",
) -> dict[str, Any]:
    """Compares row counts and financial metrics between Delta Lake and PostgreSQL."""
    spark_session = spark or get_spark_session(app_name="ParityReconciliation")

    checks: list[dict[str, Any]] = []
    has_failures = False

    print("=" * 80)
    print("RECONCILIATION: DELTA LAKE vs POSTGRESQL")
    print("=" * 80)

    # Fetch PostgreSQL metrics
    with get_db() as session:
        repo = ServingAnalyticsRepository(session)
        pg_counts = repo.get_table_counts()
        pg_totals = repo.get_financial_totals()
        pg_monthly = repo.get_monthly_revenue_summary()
        pg_category = repo.get_category_revenue_summary()
        pg_regional = repo.get_regional_top_spenders()

    # Fetch Delta Lake metrics
    spark_cust_count = spark_session.read.format("delta").load(f"{silver_dir}/customers").count()
    spark_prod_count = spark_session.read.format("delta").load(f"{silver_dir}/products").count()
    spark_date_count = spark_session.read.format("delta").load(f"{silver_dir}/date").count()
    silver_orders_df = spark_session.read.format("delta").load(f"{silver_dir}/orders")
    spark_order_count = silver_orders_df.count()

    spark_totals_row = silver_orders_df.selectExpr(
        "count(order_id) as total_orders",
        "sum(quantity) as total_quantity",
        "sum(gross_revenue) as total_gross",
        "sum(discount) as total_discount",
        "sum(net_revenue) as total_net",
    ).first()

    spark_totals = {
        "total_orders": int(spark_totals_row["total_orders"] or 0),
        "total_quantity": int(spark_totals_row["total_quantity"] or 0),
        "total_gross_revenue": Decimal(str(spark_totals_row["total_gross"] or "0.00")),
        "total_discount": Decimal(str(spark_totals_row["total_discount"] or "0.00")),
        "total_net_revenue": Decimal(str(spark_totals_row["total_net"] or "0.00")),
    }

    # Check 1: Row count parity
    count_mappings = [
        ("dim_customers", spark_cust_count, pg_counts["dim_customers"]),
        ("dim_products", spark_prod_count, pg_counts["dim_products"]),
        ("dim_date", spark_date_count, pg_counts["dim_date"]),
        ("fact_orders", spark_order_count, pg_counts["fact_orders"]),
    ]

    for table_name, spark_cnt, pg_cnt in count_mappings:
        passed = spark_cnt == pg_cnt
        if not passed:
            has_failures = True
        checks.append({
            "category": "Row Count Parity",
            "metric": f"{table_name} row count",
            "lakehouse_value": spark_cnt,
            "postgres_value": pg_cnt,
            "drift": spark_cnt - pg_cnt,
            "status": "PASSED" if passed else "FAILED",
        })

    # Check 2: Financial totals parity
    financial_mappings = [
        ("total_orders", spark_totals["total_orders"], pg_totals["total_orders"]),
        ("total_quantity", spark_totals["total_quantity"], pg_totals["total_quantity"]),
        ("total_gross_revenue ($)", spark_totals["total_gross_revenue"], pg_totals["total_gross_revenue"]),
        ("total_discount ($)", spark_totals["total_discount"], pg_totals["total_discount"]),
        ("total_net_revenue ($)", spark_totals["total_net_revenue"], pg_totals["total_net_revenue"]),
    ]

    for metric_name, spark_val, pg_val in financial_mappings:
        diff = spark_val - pg_val
        passed = diff == Decimal("0.00") if isinstance(diff, Decimal) else diff == 0
        if not passed:
            has_failures = True
        checks.append({
            "category": "Financial Parity",
            "metric": metric_name,
            "lakehouse_value": str(spark_val),
            "postgres_value": str(pg_val),
            "drift": str(diff),
            "status": "PASSED" if passed else "FAILED",
        })

    # Check 3: Monthly revenue alignment
    gold_monthly_df = spark_session.read.format("delta").load(f"{gold_dir}/monthly_revenue")
    gold_monthly_rows = {
        (int(r["cal_year"]), int(r["cal_month"])): Decimal(str(r["net_revenue"]))
        for r in gold_monthly_df.collect()
    }

    monthly_drift_total = Decimal("0.00")
    for pg_m in pg_monthly:
        key = (pg_m["cal_year"], pg_m["cal_month"])
        spark_net = gold_monthly_rows.get(key, Decimal("0.00"))
        pg_net = pg_m["net_revenue"]
        monthly_drift_total += abs(spark_net - pg_net)

    monthly_passed = (monthly_drift_total == Decimal("0.00")) and (len(gold_monthly_rows) == len(pg_monthly))
    if not monthly_passed:
        has_failures = True
    checks.append({
        "category": "Analytics Alignment",
        "metric": "Monthly Net Revenue Alignment",
        "lakehouse_value": f"{len(gold_monthly_rows)} months",
        "postgres_value": f"{len(pg_monthly)} months",
        "drift": f"${monthly_drift_total}",
        "status": "PASSED" if monthly_passed else "FAILED",
    })

    # Check 4: Category revenue alignment
    gold_cat_df = spark_session.read.format("delta").load(f"{gold_dir}/category_share")
    gold_cat_rows = {
        (int(r["cal_year"]), str(r["category"])): Decimal(str(r["category_net_revenue"]))
        for r in gold_cat_df.collect()
    }

    cat_drift_total = Decimal("0.00")
    for pg_c in pg_category:
        key = (pg_c["cal_year"], pg_c["category"])
        spark_net = gold_cat_rows.get(key, Decimal("0.00"))
        pg_net = pg_c["category_net_revenue"]
        cat_drift_total += abs(spark_net - pg_net)

    category_passed = (cat_drift_total == Decimal("0.00")) and (len(gold_cat_rows) == len(pg_category))
    if not category_passed:
        has_failures = True
    checks.append({
        "category": "Analytics Alignment",
        "metric": "Category Revenue Alignment",
        "lakehouse_value": f"{len(gold_cat_rows)} categories",
        "postgres_value": f"{len(pg_category)} categories",
        "drift": f"${cat_drift_total}",
        "status": "PASSED" if category_passed else "FAILED",
    })

    # Check 5: Regional top spenders alignment
    gold_top_df = spark_session.read.format("delta").load(f"{gold_dir}/top_customers_per_region")
    gold_top_rank1 = {
        str(r["region"]): (str(r["customer_uuid"]), Decimal(str(r["total_spend"])))
        for r in gold_top_df.filter("regional_rank = 1").collect()
    }

    regional_drift_total = Decimal("0.00")
    rankings_match = True
    for pg_r in pg_regional:
        reg = pg_r["region"]
        spark_top = gold_top_rank1.get(reg)
        if spark_top is None:
            rankings_match = False
            continue
        spark_uuid, spark_spend = spark_top
        pg_uuid = pg_r["customer_id"]
        pg_spend = pg_r["total_spend"]
        regional_drift_total += abs(spark_spend - pg_spend)
        if spark_uuid.lower() != pg_uuid.lower():
            rankings_match = False

    ranking_passed = (regional_drift_total == Decimal("0.00")) and rankings_match
    if not ranking_passed:
        has_failures = True
    checks.append({
        "category": "Analytics Alignment",
        "metric": "Regional Top Spender Alignment",
        "lakehouse_value": f"{len(gold_top_rank1)} regions",
        "postgres_value": f"{len(pg_regional)} regions",
        "drift": f"${regional_drift_total}",
        "status": "PASSED" if ranking_passed else "FAILED",
    })

    # Print summary table
    print(f"\n{'Category':<22} | {'Metric':<38} | {'Lakehouse':<18} | {'Postgres':<18} | {'Drift':<10} | {'Status'}")
    print("-" * 125)
    for c in checks:
        print(f"{c['category']:<22} | {c['metric']:<38} | {str(c['lakehouse_value']):<18} | {str(c['postgres_value']):<18} | {str(c['drift']):<10} | {c['status']}")
    print("-" * 125)

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overall_status": "FAILED" if has_failures else "PASSED",
        "total_checks": len(checks),
        "passed_checks": sum(1 for c in checks if c["status"] == "PASSED"),
        "failed_checks": sum(1 for c in checks if c["status"] == "FAILED"),
        "checks": checks,
    }

    os.makedirs(os.path.dirname(report_output_path), exist_ok=True)
    with open(report_output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"\nReconciliation report saved to: {report_output_path}")

    if has_failures:
        print("\n[ERROR] Parity reconciliation failed.")
        raise ReconciliationError("Parity reconciliation failed. See report for details.")
    else:
        print("\n[SUCCESS] All reconciliation checks passed.")

    return report


if __name__ == "__main__":
    run_reconciliation()
