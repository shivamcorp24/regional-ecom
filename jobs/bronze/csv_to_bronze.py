import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType


def ingest_raw_csv_to_bronze(
    spark: SparkSession,
    source_path: str,
    target_path: str,
    schema: StructType,
) -> int:
    """Reads a CSV with explicit string schema, appends metadata, and saves as Delta Table."""
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file does not exist: {source_path}")

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(source_path)
    )

    # Ingestion lineage metadata
    bronze_df = (
        df.withColumn("ingest_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    # Append-only write to Delta Lake
    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .save(target_path)
    )

    count = bronze_df.count()
    print(f"Bronze ingested: {count} rows -> {target_path}")
    return count

