"""Simple pytest suite for Bronze ingestion layer."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from jobs.bronze.csv_to_bronze import ingest_raw_csv_to_bronze
from jobs.bronze.pipeline import run_bronze_pipeline
from jobs.bronze.schema import CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA, RAW_ORDERS_SCHEMA
from src.spark.session import get_spark_session


@pytest.fixture(scope="session")
def spark():
    session = get_spark_session(app_name="BronzeTests", master="local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_bronze_schemas():
    """All bronze columns should be StringType."""
    for schema in (RAW_ORDERS_SCHEMA, CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA):
        assert all(isinstance(f.dataType, StringType) for f in schema.fields)


def test_missing_file_raises_error(spark: SparkSession, tmp_path):
    """Missing file should raise FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        ingest_raw_csv_to_bronze(
            spark=spark,
            source_path=str(tmp_path / "missing.csv"),
            target_path=str(tmp_path / "bronze"),
            schema=RAW_ORDERS_SCHEMA,
        )


def test_ingest_csv_to_delta(spark: SparkSession, tmp_path):
    """Ingests CSV, verifies row count, and checks lineage metadata."""
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,customer_id,customer_full_name,customer_city,product_id,product_name,product_category,unit_price,quantity,discount,order_date\n"
        "10001,1,Alice Smith,New York,101,Mouse,Electronics,29.99,2,0.00,2025-01-15\n"
    )

    target_path = (tmp_path / "bronze_orders").as_posix()
    count = ingest_raw_csv_to_bronze(spark, csv_file.as_posix(), target_path, RAW_ORDERS_SCHEMA)

    assert count == 1
    df = spark.read.format("delta").load(target_path)
    assert df.count() == 1
    assert "ingest_ts" in df.columns
    assert "source_file" in df.columns


def test_pipeline_runs_all_datasets(spark: SparkSession, tmp_path):
    """Pipeline should ingest orders, customers, and products."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    (data_dir / "raw_orders.csv").write_text("order_id\n1\n")
    (data_dir / "customers.csv").write_text("customer_id\n1\n")
    (data_dir / "products.csv").write_text("product_id\n1\n")

    warehouse_dir = tmp_path / "warehouse"
    results = run_bronze_pipeline(
        spark=spark,
        data_dir=data_dir.as_posix(),
        warehouse_dir=warehouse_dir.as_posix(),
    )

    assert results == {"orders": 1, "customers": 1, "products": 1}
    for entity in ("orders", "customers", "products"):
        delta_table = spark.read.format("delta").load((warehouse_dir / "bronze" / entity).as_posix())
        assert delta_table.count() == 1
