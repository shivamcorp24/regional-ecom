"""Runs the data processing pipeline: Bronze, Silver, Gold, Postgres load, and reconciliation."""

import os
import sys
import time
from typing import Any

from jobs.bronze.pipeline import run_bronze_pipeline
from jobs.gold.pipeline import run_gold_pipeline
from jobs.load_jdbc import run_serving_pipeline
from jobs.silver.pipeline import run_silver_pipeline
from src.reconcile import run_reconciliation
from src.spark.session import get_spark_session


def verify_landing_data_exists(data_dir: str = "data") -> None:
    """Checks that required source CSV files exist."""
    required_files = ["customers.csv", "products.csv", "raw_orders.csv"]
    missing = [f for f in required_files if not os.path.exists(os.path.join(data_dir, f))]
    if missing:
        print(f"\n[ERROR] Missing required landing files: {missing}", file=sys.stderr)
        print("Data processing cannot start without raw landed data.", file=sys.stderr)
        print("Run the data generator service first:", file=sys.stderr)
        print(f"  python -m services.data_generator.run_service --mode oneshot --output-dir {data_dir}\n", file=sys.stderr)
        raise FileNotFoundError(f"Missing required landing files: {missing}")


def run_master_pipeline(data_dir: str = "data") -> dict[str, Any]:
    """Runs Bronze, Silver, Gold, Postgres load, and reconciliation stages."""
    start_time = time.time()

    print("\n" + "=" * 80)
    print("STARTING DATA PROCESSING PIPELINE")
    print("=" * 80 + "\n")

    print(">>> [STAGE 1/5] Checking landing data files...")
    verify_landing_data_exists(data_dir=data_dir)
    print(">>> [STAGE 1/5] Landing data verified.\n")

    spark = get_spark_session(app_name="DataProcessingPipeline")

    print(">>> [STAGE 2/5] Running Bronze ingestion...")
    bronze_summary = run_bronze_pipeline(spark=spark, data_dir=data_dir)
    print(f">>> [STAGE 2/5] Completed: {bronze_summary}\n")

    print(">>> [STAGE 3/5] Running Silver transformation...")
    silver_summary = run_silver_pipeline(spark=spark)
    print(f">>> [STAGE 3/5] Completed: Valid orders={silver_summary['valid_orders_count']}, Quarantined={silver_summary['quarantined_orders_count']}\n")

    print(">>> [STAGE 4/5] Building Gold tables...")
    gold_summary = run_gold_pipeline(spark=spark)
    print(f">>> [STAGE 4/5] Completed: {gold_summary}\n")

    print(">>> [STAGE 5/5] Loading PostgreSQL serving tables...")
    serving_summary = run_serving_pipeline(spark=spark)
    print(f">>> [STAGE 5/5] Completed: Serving rows={serving_summary['serving_counts']}\n")

    print(">>> [FINAL] Running reconciliation checks...")
    reconciliation_summary = run_reconciliation(spark=spark)
    print(f">>> [FINAL] Completed: Status={reconciliation_summary['overall_status']}\n")

    total_duration = round(time.time() - start_time, 2)
    print("=" * 80)
    print(f"Pipeline completed in {total_duration} seconds.")
    print("=" * 80 + "\n")

    return {
        "duration_seconds": total_duration,
        "bronze": bronze_summary,
        "silver": silver_summary,
        "gold": gold_summary,
        "serving": serving_summary,
        "reconciliation": reconciliation_summary,
    }


if __name__ == "__main__":
    try:
        run_master_pipeline()
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}", file=sys.stderr)
        sys.exit(1)
