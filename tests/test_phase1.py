import pandas as pd
import pytest

from core.landing import verify_landing_files
from services.data_generator.generator import generate_raw_orders


def test_generator_append_keeps_prior_rows(tmp_path):
    path = str(tmp_path / "raw_orders.csv")
    generate_raw_orders(num_records=20, output_path=path)
    generate_raw_orders(num_records=10, output_path=path, start_order_id=2000, append=True, duplicate_seed=None)
    df = pd.read_csv(path)
    # 20 orders + 20 duplicates, then 10 orders + 10 duplicates, single header row.
    assert len(df) == 60
    assert set(range(2000, 2010)) <= set(df["order_id"])
    assert set(range(10001, 10021)) <= set(df["order_id"])


def test_landing_check(tmp_path):
    with pytest.raises(FileNotFoundError):
        verify_landing_files(str(tmp_path))
    for n in ["customers.csv", "products.csv", "raw_orders.csv"]:
        (tmp_path / n).write_text("a,b\n1,2\n")
    verify_landing_files(str(tmp_path))
    (tmp_path / "products.csv").write_text("")
    with pytest.raises(FileNotFoundError, match="empty"):
        verify_landing_files(str(tmp_path))
