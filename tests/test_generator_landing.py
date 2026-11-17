import pandas as pd
import pytest

from core.landing import verify_landing_files
from services.data_generator.generator import generate_raw_orders


def test_generator_append_keeps_prior_rows(tmp_path):
    path = tmp_path / "raw_orders.csv"
    generate_raw_orders(num_records=20, output_path=str(path))
    generate_raw_orders(
        num_records=10, output_path=str(path), start_order_id=2000, append=True, duplicate_seed=None
    )

    # Exactly one header line: appending must not write a second one.
    lines = path.read_text().splitlines()
    assert sum(1 for line in lines if line.startswith("order_id,")) == 1

    df = pd.read_csv(path)
    # 20 orders + 20 duplicates, then 10 orders + 10 duplicates.
    assert len(df) == 60
    assert df["order_id"].nunique() == 30
    assert set(range(10001, 10021)) <= set(df["order_id"])
    assert set(range(2000, 2010)) <= set(df["order_id"])


def test_generator_overwrite_by_default(tmp_path):
    path = tmp_path / "raw_orders.csv"
    generate_raw_orders(num_records=20, output_path=str(path))
    generate_raw_orders(num_records=10, output_path=str(path))
    assert len(pd.read_csv(path)) == 20  # 10 orders + 10 duplicates, first run replaced


def test_landing_check_reports_each_problem(tmp_path):
    names = ["customers.csv", "products.csv", "raw_orders.csv"]

    with pytest.raises(FileNotFoundError, match="missing"):
        verify_landing_files(str(tmp_path))

    for n in names:
        (tmp_path / n).write_text("a,b\n1,2\n")
    verify_landing_files(str(tmp_path))  # all good

    (tmp_path / "products.csv").write_text("")
    with pytest.raises(FileNotFoundError, match="products.csv: empty"):
        verify_landing_files(str(tmp_path))

    (tmp_path / "products.csv").write_text("a,b\n")
    with pytest.raises(FileNotFoundError, match="header only"):
        verify_landing_files(str(tmp_path))
