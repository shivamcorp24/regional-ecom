"""Date parsing and calendar dimension generation."""

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
    """Parses date strings in different formats into order_ts and integer date_key."""
    raw_date = col(date_col)

    parsed_timestamp_expr = (
        when(raw_date.rlike(r"^\d{4}-\d{2}-\d{2}$"), to_timestamp(raw_date, "yyyy-MM-dd"))
        .when(raw_date.rlike(r"^\d{2}/\d{2}/\d{4}$"), to_timestamp(raw_date, "MM/dd/yyyy"))
        .when(raw_date.rlike(r"^\d{2}-\d{2}-\d{4}$"), to_timestamp(raw_date, "dd-MM-yyyy"))
        .when(raw_date.rlike(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$"), to_timestamp(raw_date, "yyyy/MM/dd HH:mm:ss"))
        .when(raw_date.rlike(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$"), to_timestamp(raw_date, "dd/MM/yyyy HH:mm"))
        .otherwise(None)
    )

    date_key_expr = (
        when(col("order_ts").isNotNull(), date_format(col("order_ts"), "yyyyMMdd").cast("integer"))
        .otherwise(None)
    )

    return (
        df.withColumn("order_ts", parsed_timestamp_expr)
          .withColumn("date_key", date_key_expr)
    )


def generate_dense_calendar(
    spark: SparkSession,
    min_date: str | date = "2025-01-01",
    max_date: str | date = "2025-12-31",
) -> DataFrame:
    """Generates daily rows between min_date and max_date with calendar attributes."""
    start_date_str = min_date.strftime("%Y-%m-%d") if isinstance(min_date, date) else str(min_date)
    end_date_str = max_date.strftime("%Y-%m-%d") if isinstance(max_date, date) else str(max_date)

    date_sequence_df = spark.range(1).select(
        explode(
            sequence(to_date(lit(start_date_str)), to_date(lit(end_date_str)), expr("interval 1 day"))
        ).alias("full_date")
    )

    return (
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
