"""Bronze layer ingestion pipeline for CSV datasets."""

from pyspark.sql import SparkSession

from jobs.bronze.csv_to_bronze import ingest_raw_csv_to_bronze
from jobs.bronze.schema import CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA, RAW_ORDERS_SCHEMA
from src.spark.session import get_spark_session


def run_bronze_pipeline(
    spark: SparkSession | None = None,
    data_dir: str = "data",
    warehouse_dir: str = "warehouse",
) -> dict[str, int]:
    """Ingests raw CSV files into Bronze Delta tables."""
    spark = spark or get_spark_session(app_name="BronzeIngestion")

    return {
        "orders": ingest_raw_csv_to_bronze(
            spark=spark,
            source_path=f"{data_dir}/raw_orders.csv",
            target_path=f"{warehouse_dir}/bronze/orders",
            schema=RAW_ORDERS_SCHEMA,
        ),
        "customers": ingest_raw_csv_to_bronze(
            spark=spark,
            source_path=f"{data_dir}/customers.csv",
            target_path=f"{warehouse_dir}/bronze/customers",
            schema=CUSTOMERS_SCHEMA,
        ),
        "products": ingest_raw_csv_to_bronze(
            spark=spark,
            source_path=f"{data_dir}/products.csv",
            target_path=f"{warehouse_dir}/bronze/products",
            schema=PRODUCTS_SCHEMA,
        ),
    }


if __name__ == "__main__":
    run_bronze_pipeline()
