"""SparkSession configuration for Delta Lake and PostgreSQL JDBC."""

import os
import sys
from pyspark.sql import SparkSession

# On Windows, avoid conflicts between system SPARK_HOME and venv PySpark distribution
if sys.platform == "win32" and "SPARK_HOME" in os.environ:
    if not os.environ["SPARK_HOME"].startswith(sys.prefix):
        del os.environ["SPARK_HOME"]

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark_session(
    app_name: str = "RegionalEcomLakehouse",
    master: str | None = None,
) -> SparkSession:
    """Creates or retrieves a SparkSession configured with Delta Lake and PostgreSQL JDBC."""
    master_url = master or os.getenv("SPARK_MASTER", "local[*]")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )

    # Executor sizing only applies on a standalone cluster (workers offer 2 cores / 2g each).
    if master_url.startswith("spark://"):
        builder = builder.config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1g"))
        if os.getenv("SPARK_CORES_MAX"):
            # Cap total cores so one application cannot take every worker.
            builder = builder.config("spark.cores.max", os.environ["SPARK_CORES_MAX"])

    return builder.getOrCreate()


__all__ = ["get_spark_session"]
