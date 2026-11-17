"""Loads Silver Delta tables into PostgreSQL staging and swaps into serving tables."""

from typing import Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from core.config import get_settings
from core.database import get_db
from database.repository.serving import ServingRepository
from src.spark.session import get_spark_session


def get_jdbc_url_and_properties(
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> tuple[str, dict[str, str]]:
    """Builds the JDBC connection URL and connection properties."""
    settings = get_settings()
    pg_host = host or settings.postgres_host
    pg_port = port or settings.postgres_port
    pg_db = database or settings.postgres_db
    pg_user = user or settings.postgres_user
    pg_pass = password or settings.postgres_password

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    properties = {
        "user": pg_user,
        "password": pg_pass,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",
        "isolationLevel": "READ_COMMITTED",
        "truncate": "true",
        "options": "-c timezone=UTC",
    }
    return jdbc_url, properties


def write_to_jdbc_staging(
    df: DataFrame,
    table_name: str,
    jdbc_url: str,
    properties: dict[str, str],
    num_partitions: int = 4,
) -> None:
    """Writes a DataFrame to a PostgreSQL staging table."""
    (
        df.coalesce(num_partitions)
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .options(**properties)
        .mode("overwrite")
        .save()
    )


def stage_silver_to_postgres(
    spark: SparkSession,
    silver_dir: str = "warehouse/silver",
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> dict[str, int]:
    """Reads Silver Delta tables and writes them to PostgreSQL staging tables."""
    jdbc_url, properties = get_jdbc_url_and_properties(host, port, database, user, password)

    customers_df = spark.read.format("delta").load(f"{silver_dir}/customers")
    products_df = spark.read.format("delta").load(f"{silver_dir}/products")
    date_df = spark.read.format("delta").load(f"{silver_dir}/date")
    orders_df = spark.read.format("delta").load(f"{silver_dir}/orders")

    staging_dfs = {
        "stg_customers": customers_df.select(
            col("customer_uuid").alias("customer_id"),
            "first_name", "last_name", "phone", "city", "state", "region", "country",
        ),
        "stg_products": products_df.select(
            col("product_uuid").alias("product_id"),
            "product_name", "category", "unit_price",
        ),
        "stg_date": date_df.select(
            col("date_key").cast("string").alias("date_key"),
            "full_date", "cal_year", "cal_quarter", "cal_month", "cal_day", "day_of_week",
        ),
        "stg_orders": orders_df.select(
            col("order_uuid").alias("order_id"),
            col("customer_uuid").alias("customer_id"),
            col("product_uuid").alias("product_id"),
            col("date_key").cast("string").alias("date_key"),
            "order_ts", "quantity", "unit_price", "discount", "gross_revenue", "net_revenue",
        ),
    }

    # Cache each DataFrame and count once, so the Delta source is read a single time per table
    # and the counts describe exactly what is written.
    counts: dict[str, int] = {}
    try:
        for name, df in staging_dfs.items():
            df.cache()
            counts[name] = df.count()

        # The swap only runs after this function returns, so a failure on any write
        # raises here and the serving tables are never touched.
        for name, df in staging_dfs.items():
            write_to_jdbc_staging(df, name, jdbc_url, properties)
    finally:
        for df in staging_dfs.values():
            df.unpersist()

    return counts


def swap_staging_to_serving() -> dict[str, int]:
    """Transfers records from staging tables to serving tables in a single transaction."""
    with get_db() as session:
        repo = ServingRepository(session)
        repo.truncate_serving_tables()
        repo.transfer_staging_to_serving()
        serving_counts = repo.get_serving_counts()

    return serving_counts


def run_serving_pipeline(
    spark: SparkSession | None = None,
    silver_dir: str = "warehouse/silver",
) -> dict[str, Any]:
    """Stages Silver tables to PostgreSQL and swaps records into serving tables."""
    spark_session = spark or get_spark_session(app_name="ServingPipeline")

    print("Staging Silver tables to PostgreSQL staging...")
    staged_counts = stage_silver_to_postgres(spark_session, silver_dir=silver_dir)
    print("Staging completed:", staged_counts)

    print("Transferring staging data to serving tables...")
    serving_counts = swap_staging_to_serving()
    print("Serving swap completed:", serving_counts)

    return {
        "staged_counts": staged_counts,
        "serving_counts": serving_counts,
    }


if __name__ == "__main__":
    summary = run_serving_pipeline()
    print("Serving pipeline completed:", summary)
