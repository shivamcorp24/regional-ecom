"""Airflow DAG for the e-commerce data pipeline."""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from core.landing import verify_landing_files

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="regional_ecom_medallion_pipeline",
    default_args=default_args,
    description="E-commerce data processing and reconciliation pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "medallion", "delta-lake", "postgres"],
) as dag:

    check_landing_data = PythonOperator(
        task_id="check_landing_data",
        python_callable=verify_landing_files,
        op_kwargs={"data_dir": os.getenv("DATA_LANDING_DIR", "data")},
    )

    bronze_task = BashOperator(
        task_id="bronze_ingestion",
        bash_command="python -m jobs.bronze.pipeline",
        env={**os.environ, "PYTHONPATH": "."},
    )

    silver_task = BashOperator(
        task_id="silver_transformation",
        bash_command="python -m jobs.silver.pipeline",
        env={**os.environ, "PYTHONPATH": "."},
    )

    gold_task = BashOperator(
        task_id="gold_marts",
        bash_command="python -m jobs.gold.pipeline",
        env={**os.environ, "PYTHONPATH": "."},
    )

    reverse_etl_task = BashOperator(
        task_id="reverse_etl_serving",
        bash_command="python -m jobs.load_jdbc",
        env={**os.environ, "PYTHONPATH": "."},
    )

    reconcile_task = BashOperator(
        task_id="parity_reconciliation",
        bash_command="python -m src.reconcile",
        env={**os.environ, "PYTHONPATH": "."},
    )

    check_landing_data >> bronze_task >> silver_task >> gold_task >> reverse_etl_task >> reconcile_task
