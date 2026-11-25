"""Unit tests for the transform steps (src/transform.py)."""

import pytest

from src.exceptions import TransformError
from src.schema import WAREHOUSE_COLUMNS
from src.transform import clean_rows, enrich_rows, model_rows


def _row(order_id="ORD-10001", amount="100.00", region="North", channel="WEB"):
    return {
        "order_id": order_id,
        "customer_id": "CUST-1",
        "amount": amount,
        "region": region,
        "channel": channel,
        "order_date": "2026-11-22",
    }


def test_clean_rows_drops_only_bad_indices():
    rows = [_row("ORD-1"), _row("ORD-2"), _row("ORD-3")]
    assert [r["order_id"] for r in clean_rows(rows, {1})] == ["ORD-1", "ORD-3"]
    assert clean_rows(rows, set()) == rows
    assert clean_rows(rows, {0, 1, 2}) == []


def test_clean_rows_does_not_mutate_input():
    rows = [_row()]
    cleaned = clean_rows(rows, set())
    cleaned[0]["amount"] = "0"
    assert rows[0]["amount"] == "100.00"


def test_enrich_normalises_and_derives_amount_usd():
    enriched = enrich_rows([_row()])
    assert enriched[0]["region"] == "north"
    assert enriched[0]["channel"] == "web"
    assert enriched[0]["amount_usd"] == "100.00"
    assert enriched[0]["amount"] == "100.00"


def test_enrich_formats_amount_to_two_decimals():
    assert enrich_rows([_row(amount="5")])[0]["amount_usd"] == "5.00"
    assert enrich_rows([_row(amount="5.1")])[0]["amount_usd"] == "5.10"


def test_enrich_rejects_unknown_region_and_channel():
    with pytest.raises(TransformError, match="unknown region"):
        enrich_rows([_row(region="central")])
    with pytest.raises(TransformError, match="unknown channel"):
        enrich_rows([_row(channel="phone")])


def test_enrich_rejects_non_numeric_amount():
    with pytest.raises(TransformError, match="not a number"):
        enrich_rows([_row(amount="free")])


def test_model_projects_to_warehouse_columns_in_order():
    modeled = model_rows(enrich_rows([_row()]))
    assert list(modeled[0].keys()) == list(WAREHOUSE_COLUMNS)
    assert modeled[0] == {
        "order_id": "ORD-10001",
        "customer_id": "CUST-1",
        "amount_usd": "100.00",
        "region": "north",
        "channel": "web",
        "order_date": "2026-11-22",
    }


def test_model_raises_when_a_column_is_missing():
    with pytest.raises(TransformError, match="missing column"):
        model_rows([_row()])
