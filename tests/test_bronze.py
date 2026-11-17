from jobs.bronze.csv_to_bronze import ingest_raw_csv_to_bronze
from jobs.bronze.schema import CUSTOMERS_SCHEMA

HEADER = "customer_id,first_name,last_name,phone,city,state,region,country\n"


def _row(cid: int) -> str:
    return f"{cid},First{cid},Last{cid},555-000{cid},Chicago,IL,Midwest,USA\n"


def test_rerun_on_same_file_adds_nothing(spark, tmp_path):
    src = tmp_path / "customers.csv"
    target = str(tmp_path / "bronze_customers")
    src.write_text(HEADER + _row(1) + _row(2))

    first = ingest_raw_csv_to_bronze(spark, str(src), target, CUSTOMERS_SCHEMA, key_col="customer_id")
    second = ingest_raw_csv_to_bronze(spark, str(src), target, CUSTOMERS_SCHEMA, key_col="customer_id")

    assert (first, second) == (2, 0)
    assert spark.read.format("delta").load(target).count() == 2


def test_new_keys_are_appended_old_ones_skipped(spark, tmp_path):
    src = tmp_path / "customers.csv"
    target = str(tmp_path / "bronze_customers")
    src.write_text(HEADER + _row(1) + _row(2))
    ingest_raw_csv_to_bronze(spark, str(src), target, CUSTOMERS_SCHEMA, key_col="customer_id")

    src.write_text(HEADER + _row(1) + _row(2) + _row(3))  # feed grew by one row
    added = ingest_raw_csv_to_bronze(spark, str(src), target, CUSTOMERS_SCHEMA, key_col="customer_id")

    assert added == 1
    ids = {r["customer_id"] for r in spark.read.format("delta").load(target).collect()}
    assert ids == {"1", "2", "3"}


def test_without_key_col_it_still_appends_everything(spark, tmp_path):
    src = tmp_path / "customers.csv"
    target = str(tmp_path / "bronze_customers")
    src.write_text(HEADER + _row(1))

    ingest_raw_csv_to_bronze(spark, str(src), target, CUSTOMERS_SCHEMA)
    ingest_raw_csv_to_bronze(spark, str(src), target, CUSTOMERS_SCHEMA)

    assert spark.read.format("delta").load(target).count() == 2
