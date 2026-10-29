"""Date transformations: shape-based regex date parsing and dense calendar dimension."""

from datetime import date
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    explode,
    expr,
    lit,
    month,
    quarter,
    sequence,
    to_date,
    to_timestamp,
    when,
    year,
)


def parse_mixed_dates(df: DataFrame, date_col: str = "order_date") -> DataFrame:
    """Parses 5 distinct date formats using shape-based regex matching into timestamps and date keys.

    Supported Date Patterns:
      1. YYYY-MM-DD          -> yyyy-MM-dd           (e.g., 2025-01-15)
      2. MM/DD/YYYY          -> MM/dd/yyyy           (e.g., 03/20/2025)
      3. DD-MM-YYYY          -> dd-MM-yyyy           (e.g., 25-07-2025)
      4. YYYY/MM/DD HH:MM:SS -> yyyy/MM/dd HH:mm:ss  (e.g., 2025/11/05 14:30:45)
      5. DD/MM/YYYY HH:MM    -> dd/MM/yyyy HH:mm     (e.g., 10/12/2025 09:15)

    Returns:
        DataFrame: Enriched with `order_ts` (TimestampType) and `date_key` (IntegerType YYYYMMDD).
    """
    raw_date = col(date_col)

    # 1. Match regex shape to corresponding timestamp format
    parsed_timestamp_expr = (
        when(raw_date.rlike(r"^\d{4}-\d{2}-\d{2}$"), to_timestamp(raw_date, "yyyy-MM-dd"))
        .when(raw_date.rlike(r"^\d{2}/\d{2}/\d{4}$"), to_timestamp(raw_date, "MM/dd/yyyy"))
        .when(raw_date.rlike(r"^\d{2}-\d{2}-\d{4}$"), to_timestamp(raw_date, "dd-MM-yyyy"))
        .when(raw_date.rlike(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$"), to_timestamp(raw_date, "yyyy/MM/dd HH:mm:ss"))
        .when(raw_date.rlike(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$"), to_timestamp(raw_date, "dd/MM/yyyy HH:mm"))
        .otherwise(None)
    )

    # 2. Derive integer surrogate key (YYYYMMDD) from parsed timestamp
    date_key_expr = (
        when(col("order_ts").isNotNull(), date_format(col("order_ts"), "yyyyMMdd").cast("integer"))
        .otherwise(None)
    )

    # 3. Apply transformations sequentially
    transformed_df = (
        df.withColumn("order_ts", parsed_timestamp_expr)
          .withColumn("date_key", date_key_expr)
    )

    return transformed_df


def generate_dense_calendar(
    spark: SparkSession,
    min_date: str | date = "2025-01-01",
    max_date: str | date = "2025-12-31",
) -> DataFrame:
    """Generates a continuous, gap-free calendar dimension between min_date and max_date.

    Returns:
        DataFrame: Conforming to SILVER_DATE_SCHEMA with date_key, full_date, cal_year,
                   cal_quarter, cal_month, cal_day, and day_of_week.
    """
    # 1. Format date bounds as strings (YYYY-MM-DD)
    start_date_str = min_date.strftime("%Y-%m-%d") if isinstance(min_date, date) else str(min_date)
    end_date_str = max_date.strftime("%Y-%m-%d") if isinstance(max_date, date) else str(max_date)

    # 2. Generate a continuous daily sequence using Spark sequence()
    date_sequence_df = spark.range(1).select(
        explode(
            sequence(to_date(lit(start_date_str)), to_date(lit(end_date_str)), expr("interval 1 day"))
        ).alias("full_date")
    )

    # 3. Extract calendar attributes
    calendar_df = (
        date_sequence_df
        .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("integer"))
        .withColumn("cal_year", year(col("full_date")).cast("integer"))
        .withColumn("cal_quarter", quarter(col("full_date")).cast("integer"))
        .withColumn("cal_month", month(col("full_date")).cast("integer"))
        .withColumn("cal_day", dayofmonth(col("full_date")).cast("integer"))
        .withColumn("day_of_week", date_format(col("full_date"), "EEEE"))
        .select(
            "date_key",
            "full_date",
            "cal_year",
            "cal_quarter",
            "cal_month",
            "cal_day",
            "day_of_week",
        )
    )

    return calendar_df
