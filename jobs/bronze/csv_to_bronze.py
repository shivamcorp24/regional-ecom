import os
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType


def ingest_raw_csv_to_bronze(
    spark: SparkSession,
    source_path: str,
    target_path: str,
    schema: StructType,
    key_col: str | None = None,
) -> int:
    """Reads a CSV file, adds ingest metadata, and appends new rows to a Delta table.

    If key_col is given and the target already exists, rows whose key is already present
    in Bronze are skipped, so re-running on the same landing file does not duplicate data.
    """
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file does not exist: {source_path}")

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load(source_path)
    )

    bronze_df = (
        df.withColumn("ingest_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    if key_col and DeltaTable.isDeltaTable(spark, target_path):
        existing_keys = spark.read.format("delta").load(target_path).select(key_col).distinct()
        bronze_df = bronze_df.join(existing_keys, on=key_col, how="left_anti")

    bronze_df = bronze_df.cache()
    count = bronze_df.count()

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .save(target_path)
    )

    bronze_df.unpersist()
    print(f"Bronze ingested: {count} rows -> {target_path}")
    return count
