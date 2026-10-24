from pyspark.sql import SparkSession

from src.spark.session import get_spark_session
from jobs.bronze.csv_to_bronze import ingest_raw_csv_to_bronze
from jobs.bronze.schema import RAW_ORDERS_SCHEMA, CUSTOMERS_SCHEMA, PRODUCTS_SCHEMA

def run_bronze_pipeline(
    spark: SparkSession | None = None,
    data_dir: str = "data",
    warehouse_dir: str = "warehouse",
) -> dict[str, int]:
    """Runs the bronze ingestion stage for all three source datasets."""

    spark = spark or get_spark_session(app_name="BronzeIngestion")

    results = {
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

    return results


if __name__ == "__main__":
    run_bronze_pipeline()


