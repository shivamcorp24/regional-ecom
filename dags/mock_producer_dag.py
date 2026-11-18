"""Airflow DAG to trigger the synthetic data generator."""

from datetime import datetime

from airflow import DAG

from ecom_dag_utils import default_args, python_module_task

with DAG(
    dag_id="upstream_ecom_data_producer",
    default_args=default_args,
    description="Generates synthetic e-commerce data into landing folder",
    schedule_interval="*/2 * * * *",  # new batch every 2 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["producer", "data-generator"],
) as dag:

    generate_landing_batch = python_module_task(
        "generate_mock_source_feed",
        "services.data_generator.run_service",
        "--mode append --records 5000 --output-dir data",
    )
