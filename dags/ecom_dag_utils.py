"""Shared settings and helpers for the e-commerce DAGs."""

import os
from datetime import timedelta

from airflow.operators.bash import BashOperator

# Absolute locations inside the Airflow container (the repo is mounted at /opt/spark-app).
PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/spark-app")
DATA_DIR = os.getenv("DATA_LANDING_DIR", f"{PROJECT_DIR}/data")
# Interpreter of the project virtualenv (Airflow's own environment lacks Spark and SQLAlchemy 2).
PYTHON = os.getenv("ECOM_PYTHON", "python")

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def python_module_task(task_id: str, module: str, args: str = "") -> BashOperator:
    """Runs `python -m <module>` from the project directory with the project interpreter."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} -m {module} {args}".strip(),
        env={**os.environ, "PYTHONPATH": PROJECT_DIR},
    )
