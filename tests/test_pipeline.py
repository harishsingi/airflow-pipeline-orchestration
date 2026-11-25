"""End-to-end tests for the daily pipeline (src/pipeline.py)."""

from pathlib import Path

import pytest

from src.config import load_config
from src.exceptions import PipelineError, SchemaError
from src.generate_data import generate_orders
from src.pipeline import run_daily_pipeline
from src.schema import ORDER_COLUMNS
from src.storage import read_csv, write_csv_atomic


def _write_source(tmp_path, rows):
    source = tmp_path / "orders.csv"
    write_csv_atomic(source, rows, ORDER_COLUMNS)
    return source


def test_full_run_counts_and_quarantine(tmp_path):
    source = _write_source(tmp_path, generate_orders())
    out = tmp_path / "out"
    result = run_daily_pipeline(source, out, "2026-11-22", load_config())
    assert result.rows_in == 121
    assert result.rows_clean == 118
    assert result.rows_rejected == 3
    assert result.rows_loaded == 118
    assert result.failures_by_rule == {
        "not_null:customer_id": 1,
        "unique:order_id": 1,
        "range:amount": 1,
    }
    warehouse = read_csv(out / "warehouse_orders.csv")
    assert len(warehouse) == 118
    assert {r["order_id"] for r in warehouse} == {f"ORD-{10000 + i}" for i in range(1, 119)}
    assert all("amount_usd" in r for r in warehouse)
    quarantine = read_csv(out / "quarantine.csv")
    assert len(quarantine) == 3
    assert {r["order_id"] for r in quarantine} == {"ORD-10119", "ORD-10001", "ORD-10120"}
    # The duplicate ORD-10001 is quarantined, but its first occurrence
    # is a good row and is the one that loads.
    assert "ORD-10001" in {r["order_id"] for r in warehouse}


def test_rerun_is_idempotent(tmp_path):
    source = _write_source(tmp_path, generate_orders())
    out = tmp_path / "out"
    first = run_daily_pipeline(source, out, "2026-11-22", load_config())
    warehouse_before = (out / "warehouse_orders.csv").read_bytes()
    quarantine_before = (out / "quarantine.csv").read_bytes()
    second = run_daily_pipeline(source, out, "2026-11-22", load_config())
    assert second == first
    assert (out / "warehouse_orders.csv").read_bytes() == warehouse_before
    assert (out / "quarantine.csv").read_bytes() == quarantine_before


def test_all_clean_run_writes_no_quarantine_file(tmp_path):
    source = _write_source(
        tmp_path, generate_orders(rows=5, include_bad_rows=False)
    )
    out = tmp_path / "out"
    result = run_daily_pipeline(source, out, "2026-11-22", load_config())
    assert result.rows_rejected == 0
    assert result.rows_loaded == 5
    assert not (out / "quarantine.csv").exists()


def test_all_bad_run_loads_header_only_not_crash(tmp_path):
    rows = [
        {
            "order_id": "ORD-1",
            "customer_id": "",
            "amount": "99999",
            "region": "north",
            "channel": "web",
            "order_date": "2026-11-22",
        }
    ]
    source = _write_source(tmp_path, rows)
    out = tmp_path / "out"
    result = run_daily_pipeline(source, out, "2026-11-22", load_config())
    assert result.rows_loaded == 0
    assert result.rows_rejected == 1
    assert (out / "warehouse_orders.csv").exists()
    assert read_csv(out / "warehouse_orders.csv") == []


def test_missing_source_raises(tmp_path):
    with pytest.raises(PipelineError, match="not found"):
        run_daily_pipeline(tmp_path / "missing.csv", tmp_path / "out", "2026-11-22")


def test_bad_schema_raises_before_any_output(tmp_path):
    # A file genuinely missing the region column (not just an empty value).
    source = tmp_path / "orders.csv"
    source.write_text(
        "order_id,customer_id,amount,channel,order_date\n"
        "ORD-10001,CUST-1,10.00,web,2026-11-22\n"
    )
    out = tmp_path / "out"
    with pytest.raises(SchemaError):
        run_daily_pipeline(source, out, "2026-11-22", load_config())
    assert not (out / "warehouse_orders.csv").exists()


def test_invalid_run_date_raises(tmp_path):
    source = _write_source(tmp_path, generate_orders(rows=2, include_bad_rows=False))
    with pytest.raises(PipelineError, match="YYYY-MM-DD"):
        run_daily_pipeline(source, tmp_path / "out", "22-11-2026")
