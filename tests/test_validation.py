"""Unit tests for the data-quality rule engine (src/validation.py)."""

import pytest

from src.config import DataQualityRules, validate_rules
from src.exceptions import ConfigError
from src.validation import validate_rows

RULES = DataQualityRules(
    not_null=("order_id", "customer_id", "amount"),
    unique=("order_id",),
    range={"amount": (0.0, 10000.0)},
)


def _row(order_id="ORD-10001", customer_id="CUST-1", amount="100.00"):
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "amount": amount,
        "region": "north",
        "channel": "web",
        "order_date": "2026-11-22",
    }


def test_clean_rows_pass_with_zero_rejected():
    result = validate_rows([_row(), _row("ORD-10002")], RULES)
    assert result.passed
    assert result.rows_in == 2
    assert result.rows_clean == 2
    assert result.rows_rejected == 0
    assert result.failures_by_rule == {}


def test_empty_input_passes_with_zero_rows():
    result = validate_rows([], RULES)
    assert result.passed
    assert result.rows_in == 0


def test_not_null_blank_and_missing_customer():
    rows = [_row(), _row("ORD-10002", customer_id=""), _row("ORD-10003", customer_id="   ")]
    result = validate_rows(rows, RULES)
    assert result.rows_rejected == 2
    assert result.failures_by_rule == {"not_null:customer_id": 2}
    assert result.bad_indices == frozenset({1, 2})


def test_unique_keeps_first_occurrence_only():
    rows = [_row(), _row(), _row("ORD-10002")]
    result = validate_rows(rows, RULES)
    assert result.rows_rejected == 1
    assert result.bad_indices == frozenset({1})
    assert result.failures_by_rule == {"unique:order_id": 1}


def test_range_bounds_are_inclusive():
    rows = [_row("ORD-1", amount="0"), _row("ORD-2", amount="10000"), _row("ORD-3", amount="10000.01")]
    result = validate_rows(rows, RULES)
    assert result.rows_rejected == 1
    assert result.bad_indices == frozenset({2})


def test_range_negative_and_non_numeric_fail_without_crashing():
    rows = [_row("ORD-1", amount="-5"), _row("ORD-2", amount="not-a-number")]
    result = validate_rows(rows, RULES)
    assert result.rows_rejected == 2
    assert result.failures_by_rule == {"range:amount": 2}


def test_one_row_can_break_multiple_rules():
    rows = [_row("ORD-1", customer_id="", amount="99999")]
    result = validate_rows(rows, RULES)
    assert result.rows_rejected == 1
    assert len(result.failures) == 2
    assert result.rows_clean == 0


def test_metrics_add_up():
    rows = [_row(), _row("ORD-10002", customer_id=""), _row("ORD-10001")]
    result = validate_rows(rows, RULES)
    assert result.rows_in == 3
    assert result.rows_clean + result.rows_rejected == result.rows_in
    assert sum(result.failures_by_rule.values()) == len(result.failures)


def test_validation_never_mutates_input():
    rows = [_row(customer_id="")]
    snapshot = [dict(r) for r in rows]
    validate_rows(rows, RULES)
    assert rows == snapshot


def test_rule_on_unknown_column_is_a_config_error():
    bad = DataQualityRules(not_null=("no_such_column",))
    with pytest.raises(ConfigError, match="unknown column"):
        validate_rows([_row()], bad)


def test_validate_rules_rejects_unknown_keys():
    with pytest.raises(ConfigError, match="unknown data-quality rule"):
        validate_rules({"not_null": ["order_id"], "bogus": ["x"]})


def test_validate_rules_rejects_reversed_range():
    with pytest.raises(ConfigError):
        validate_rules({"range": {"amount": [10, 1]}})


def test_split_clean_and_quarantine():
    from src.validation import split_clean_and_quarantine

    rows = [_row(), _row("ORD-10002", customer_id=""), _row("ORD-10001")]
    result = validate_rows(rows, RULES)
    clean, quarantined = split_clean_and_quarantine(rows, result)
    assert [r["order_id"] for r in clean] == ["ORD-10001"]
    assert [r["order_id"] for r in quarantined] == ["ORD-10002", "ORD-10001"]
    # Copies, not aliases of the input rows.
    clean[0]["amount"] = "0"
    assert rows[0]["amount"] == "100.00"
