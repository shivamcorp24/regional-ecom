"""Shared fixtures."""

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession with Delta Lake, shared by all Spark tests (startup is slow)."""
    from src.spark.session import get_spark_session

    session = get_spark_session(app_name="tests", master="local[2]")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()
