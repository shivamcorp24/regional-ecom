"""Airflow DAG to trigger the synthetic data generator."""

from datetime import datetime

from airflow import DAG

from ecom_dag_utils import default_args, python_module_task

with DAG(
    dag_id="upstream_ecom_data_producer",
    default_args=default_args,
    description="Generates synthetic e-commerce data into landing folder",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["producer", "data-generator"],
) as dag:

    generate_landing_batch = python_module_task(
        "generate_mock_source_feed",
        "services.data_generator.run_service",
        "--mode oneshot --output-dir data",
    )
