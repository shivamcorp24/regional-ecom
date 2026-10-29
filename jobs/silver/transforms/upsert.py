"""Delta Lake ACID upserts (MERGE INTO), appends, and compaction (OPTIMIZE ZORDER)."""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def upsert_delta_table(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame,
    merge_keys: str | list[str],
) -> None:
    """Executes an idempotent Delta Lake MERGE INTO upsert.

    If the target Delta table does not exist:
        Writes source DataFrame as the initial Delta table.
    If the target Delta table exists:
        Matches on merge_keys and updates matching rows / inserts new rows.
    """
    key_list = [merge_keys] if isinstance(merge_keys, str) else merge_keys

    # 1. Initialize table if it doesn't already exist
    if not DeltaTable.isDeltaTable(spark, target_path):
        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )
        return

    # 2. Build merge condition: e.g. "target.customer_id = source.customer_id"
    join_clauses = [f"target.{key} = source.{key}" for key in key_list]
    merge_condition = " AND ".join(join_clauses)

    # 3. Execute ACID merge
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
    """Appends records to a Delta table (used for append-only audit & quarantine logs)."""
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
    """Executes Delta OPTIMIZE file compaction, optionally with Z-ORDER clustering."""
    if not DeltaTable.isDeltaTable(spark, target_path):
        return

    delta_table = DeltaTable.forPath(spark, target_path)

    if zorder_cols:
        delta_table.optimize().executeZOrderBy(*zorder_cols)
    else:
        delta_table.optimize().executeCompaction()
