"""Shared validation of the raw landing files used by the CLI and Airflow entry points."""

import os

REQUIRED_LANDING_FILES = ["customers.csv", "products.csv", "raw_orders.csv"]


def verify_landing_files(data_dir: str = "data", required: list[str] | None = None) -> None:
    """Raises FileNotFoundError unless every landing file exists, is non-empty and has data rows."""
    problems = []
    for name in required or REQUIRED_LANDING_FILES:
        path = os.path.join(data_dir, name)
        if not os.path.exists(path):
            problems.append(f"{name}: missing")
        elif os.path.getsize(path) == 0:
            problems.append(f"{name}: empty")
        else:
            with open(path, encoding="utf-8") as f:
                f.readline()  # header
                if not f.readline():
                    problems.append(f"{name}: header only, no data rows")

    if problems:
        raise FileNotFoundError(
            f"Landing files in '{data_dir}' are not ready: {problems}. "
            "Run the data generator first: "
            f"python -m services.data_generator.run_service --mode oneshot --output-dir {data_dir}"
        )
