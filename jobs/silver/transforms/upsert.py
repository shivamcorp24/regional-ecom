"""Delta Lake upsert and optimization helper functions."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def upsert_delta_table(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame,
    merge_keys: str | list[str],
) -> None:
    """Inserts or updates rows in a Delta table based on merge keys."""
    key_list = [merge_keys] if isinstance(merge_keys, str) else merge_keys

    if not DeltaTable.isDeltaTable(spark, target_path):
        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )
        return

    join_clauses = [f"target.{key} = source.{key}" for key in key_list]
    merge_condition = " AND ".join(join_clauses)

    delta_target = DeltaTable.forPath(spark, target_path)
    (
        delta_target.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def append_delta_table(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame,
) -> None:
    """Appends records to a Delta table."""
    (
        source_df.write
        .format("delta")
        .mode("append")
        .save(target_path)
    )


def optimize_delta_table(
    spark: SparkSession,
    target_path: str,
    zorder_cols: list[str] | None = None,
) -> None:
    """Runs compaction and optional Z-ordering on a Delta table."""
    if not DeltaTable.isDeltaTable(spark, target_path):
        return

    delta_table = DeltaTable.forPath(spark, target_path)

    if zorder_cols:
        delta_table.optimize().executeZOrderBy(*zorder_cols)
    else:
        delta_table.optimize().executeCompaction()
