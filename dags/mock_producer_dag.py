"""Airflow DAG to trigger the synthetic data generator."""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="upstream_ecom_data_producer",
    default_args=default_args,
    description="Generates synthetic e-commerce data into landing folder",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["producer", "data-generator"],
) as dag:

    generate_landing_batch = BashOperator(
        task_id="generate_mock_source_feed",
        bash_command="python -m services.data_generator.run_service --mode oneshot --output-dir data",
        env={**os.environ, "PYTHONPATH": "."},
    )
