"""Unit tests for CSV storage helpers (src/storage.py)."""

import pytest

from src.exceptions import StorageError
from src.schema import ORDER_COLUMNS
from src.storage import read_csv, write_csv_atomic


def _rows():
    return [
        {
            "order_id": "ORD-10001",
            "customer_id": "CUST-1",
            "amount": "10.00",
            "region": "north",
            "channel": "web",
            "order_date": "2026-11-22",
        }
    ]


def test_write_then_read_roundtrip(tmp_path):
    path = tmp_path / "out.csv"
    assert write_csv_atomic(path, _rows(), ORDER_COLUMNS) == 1
    assert read_csv(path) == _rows()


def test_write_creates_parent_directories(tmp_path):
    path = tmp_path / "nested" / "deep" / "out.csv"
    write_csv_atomic(path, _rows(), ORDER_COLUMNS)
    assert path.is_file()


def test_write_empty_rows_still_writes_header(tmp_path):
    path = tmp_path / "empty.csv"
    write_csv_atomic(path, [], ORDER_COLUMNS)
    assert path.read_text().strip() == ",".join(ORDER_COLUMNS)
    assert read_csv(path) == []


def test_overwrite_is_idempotent(tmp_path):
    path = tmp_path / "out.csv"
    write_csv_atomic(path, _rows(), ORDER_COLUMNS)
    first = path.read_bytes()
    write_csv_atomic(path, _rows(), ORDER_COLUMNS)
    assert path.read_bytes() == first


def test_read_missing_file_raises_storage_error(tmp_path):
    with pytest.raises(StorageError, match="not found"):
        read_csv(tmp_path / "missing.csv")


def test_no_temp_files_left_behind(tmp_path):
    path = tmp_path / "out.csv"
    write_csv_atomic(path, _rows(), ORDER_COLUMNS)
    assert list(tmp_path.iterdir()) == [path]
