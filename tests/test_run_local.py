"""Tests for the local runner CLI (src/run_local.py)."""

from src.generate_data import generate_orders
from src.run_local import main
from src.schema import ORDER_COLUMNS
from src.storage import write_csv_atomic


def test_run_local_succeeds_and_writes_outputs(tmp_path, caplog):
    write_csv_atomic(tmp_path / "orders.csv", generate_orders(), ORDER_COLUMNS)
    assert main(["--date", "2026-11-22"], data_dir=tmp_path) == 0
    assert (tmp_path / "warehouse_orders.csv").is_file()
    assert (tmp_path / "quarantine.csv").is_file()
    assert (tmp_path / "staging_orders.csv").is_file()


def test_run_local_missing_input_returns_1(tmp_path):
    assert main(["--date", "2026-11-22"], data_dir=tmp_path) == 1
    assert not (tmp_path / "warehouse_orders.csv").exists()
