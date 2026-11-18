"""Airflow DAG for the e-commerce data pipeline."""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from core.landing import verify_landing_files
from ecom_dag_utils import DATA_DIR, default_args, python_module_task

with DAG(
    dag_id="regional_ecom_medallion_pipeline",
    default_args=default_args,
    description="E-commerce data processing and reconciliation pipeline",
    schedule_interval="*/30 * * * *",  # ingest new landing data every 30 minutes,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "medallion", "delta-lake", "postgres"],
) as dag:

    check_landing_data = PythonOperator(
        task_id="check_landing_data",
        python_callable=verify_landing_files,
        op_kwargs={"data_dir": DATA_DIR},
    )

    # Creates/updates the serving schema (idempotent); a fresh Postgres volume has no tables yet.
    migrate_task = python_module_task("migrate_warehouse", "alembic", "upgrade head")

    bronze_task = python_module_task("bronze_ingestion", "jobs.bronze.pipeline")
    silver_task = python_module_task("silver_transformation", "jobs.silver.pipeline")
    gold_task = python_module_task("gold_marts", "jobs.gold.pipeline")
    reverse_etl_task = python_module_task("reverse_etl_serving", "jobs.load_jdbc")
    reconcile_task = python_module_task("parity_reconciliation", "src.reconcile")

    check_landing_data >> migrate_task >> bronze_task >> silver_task >> gold_task >> reverse_etl_task >> reconcile_task
