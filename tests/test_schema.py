"""Unit tests for schema validation (src/schema.py)."""

import pytest

from src.exceptions import SchemaError
from src.schema import ORDER_COLUMNS, validate_schema


def _rows(n=2):
    return [
        {
            "order_id": f"ORD-1000{i}",
            "customer_id": "CUST-1",
            "amount": "10.00",
            "region": "north",
            "channel": "web",
            "order_date": "2026-11-22",
        }
        for i in range(1, n + 1)
    ]


def test_valid_rows_pass():
    validate_schema(_rows())


def test_no_rows_raises():
    with pytest.raises(SchemaError, match="no data rows"):
        validate_schema([])


def test_missing_column_raises():
    rows = _rows()
    del rows[0]["region"]
    with pytest.raises(SchemaError, match="missing required"):
        validate_schema(rows)


def test_unexpected_column_raises():
    rows = _rows()
    rows[0]["surprise"] = "x"
    with pytest.raises(SchemaError, match="unexpected column"):
        validate_schema(rows)


def test_non_numeric_amount_raises():
    rows = _rows()
    rows[1]["amount"] = "free"
    with pytest.raises(SchemaError, match="not a number"):
        validate_schema(rows)


def test_bad_date_format_raises():
    rows = _rows()
    rows[0]["order_date"] = "22/11/2026"
    with pytest.raises(SchemaError, match="not YYYY-MM-DD"):
        validate_schema(rows)


def test_columns_constant_matches_expected_order():
    assert ORDER_COLUMNS == (
        "order_id",
        "customer_id",
        "amount",
        "region",
        "channel",
        "order_date",
    )
