"""SparkSession builder configured for Delta Lake and PostgreSQL JDBC."""

import os
import sys
from pyspark.sql import SparkSession

# On Windows, system SPARK_HOME (e.g. C:\spark) often conflicts with the venv's PySpark/Scala version.
# Ensure PySpark uses the virtual environment's bundled distribution instead.
if sys.platform == "win32" and "SPARK_HOME" in os.environ:
    if not os.environ["SPARK_HOME"].startswith(sys.prefix):
        del os.environ["SPARK_HOME"]


def get_spark_session(
    app_name: str = "RegionalEcomLakehouse",
    master: str | None = None,
) -> SparkSession:
    """Build or retrieve a SparkSession configured for Delta Lake.

    Configures:
    - Delta Lake ACID extensions & catalog (DeltaCatalog)
    - PostgreSQL JDBC driver package
    - Bounded shuffle partitions (preventing small-file sprawl)
    - UTC timezone consistency
    """
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
    )

    return builder.getOrCreate()


__all__ = ["get_spark_session"]
