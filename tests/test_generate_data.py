"""Unit tests for the synthetic data generator (src/generate_data.py)."""

import pytest

from src.exceptions import PipelineError
from src.generate_data import generate_orders
from src.schema import validate_schema


def test_default_output_is_121_rows_with_3_bad():
    rows = generate_orders()
    assert len(rows) == 121
    validate_schema(rows)  # schema-valid; the bad rows fail rules, not schema
    bad_customer = [r for r in rows if not r["customer_id"]]
    assert len(bad_customer) == 1
    ids = [r["order_id"] for r in rows]
    assert len(ids) - len(set(ids)) == 1  # exactly one duplicate id
    assert any(float(r["amount"]) > 10000 for r in rows)


def test_same_seed_is_deterministic():
    assert generate_orders(seed=7) == generate_orders(seed=7)
    assert generate_orders(seed=7) != generate_orders(seed=8)


def test_custom_rows_date_and_no_bad_rows():
    rows = generate_orders(rows=5, run_date="2026-11-23", include_bad_rows=False)
    assert len(rows) == 5
    assert all(r["order_date"] == "2026-11-23" for r in rows)
    assert len({r["order_id"] for r in rows}) == 5


def test_bad_rows_are_the_documented_three():
    rows = generate_orders(rows=10)
    bad = rows[10:]
    assert bad[0]["customer_id"] == ""
    assert bad[1]["order_id"] == "ORD-10001"  # duplicate of the first row
    assert float(bad[2]["amount"]) == 25000.0


def test_invalid_rows_and_date_raise():
    with pytest.raises(PipelineError):
        generate_orders(rows=0)
    with pytest.raises(PipelineError):
        generate_orders(run_date="not-a-date")
