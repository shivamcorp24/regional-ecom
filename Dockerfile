# Airflow image able to run the Spark/Delta job modules.
# Airflow 2.9 pins SQLAlchemy 1.4 and an older pandas, which conflict with the project's
# dependencies, so the project gets its own virtualenv and the DAGs call that interpreter.
FROM apache/airflow:2.9.3

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean

USER airflow
# Keep in sync with [project].dependencies in pyproject.toml.
RUN python -m venv /home/airflow/ecom-venv \
    && /home/airflow/ecom-venv/bin/pip install --no-cache-dir \
        "alembic>=1.20.0" \
        "numpy>=2.4.6" \
        "pandas>=3.0.6" \
        "psycopg2-binary>=2.9.13" \
        "pydantic-settings>=2.15.0" \
        "pyspark>=3.5.1,<3.6.0" \
        "delta-spark>=3.2.0,<3.3.0" \
        "sqlalchemy>=2.1.1"

# Stable path to the pip-installed Spark distribution, so compose can start the standalone
# master/worker (bin/spark-class) from this same image.
RUN ln -s "$(/home/airflow/ecom-venv/bin/python -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')" /home/airflow/spark

ENV ECOM_PYTHON=/home/airflow/ecom-venv/bin/python
ENV SPARK_HOME=/home/airflow/spark
