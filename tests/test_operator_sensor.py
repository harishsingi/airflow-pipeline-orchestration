"""Tests for the custom operator and sensor, importable without Airflow.

Airflow is not installed in this environment, so these tests exercise
the real classes through the stub base classes in the plugin modules.
"""

import pytest

from plugins.operators.data_quality_operator import DataQualityOperator
from plugins.sensors.partition_sensor import PartitionSensor
from src.config import load_config
from src.exceptions import ConfigError, DataQualityError
from src.generate_data import generate_orders
from src.schema import ORDER_COLUMNS
from src.storage import write_csv_atomic

RULES = load_config().data_quality.to_dict()


def _write_orders(tmp_path, rows):
    path = tmp_path / "orders.csv"
    write_csv_atomic(path, rows, ORDER_COLUMNS)
    return str(path)


def test_operator_passes_clean_data(tmp_path):
    path = _write_orders(tmp_path, generate_orders(rows=5, include_bad_rows=False))
    result = DataQualityOperator(
        task_id="validate", dataset=path, rules=RULES
    ).execute({})
    assert result["status"] == "PASS"
    assert result["rows"] == 5
    assert result["rows_rejected"] == 0


def test_operator_fails_on_bad_data_with_counts(tmp_path):
    path = _write_orders(tmp_path, generate_orders())
    operator = DataQualityOperator(task_id="validate", dataset=path, rules=RULES)
    with pytest.raises(DataQualityError) as excinfo:
        operator.execute({})
    error = excinfo.value
    assert error.rows_in == 121
    assert error.rows_rejected == 3
    assert error.failures_by_rule == {
        "not_null:customer_id": 1,
        "unique:order_id": 1,
        "range:amount": 1,
    }
    assert "data quality failed" in str(error)


def test_operator_tolerated_failure_returns_result(tmp_path):
    path = _write_orders(tmp_path, generate_orders())
    result = DataQualityOperator(
        task_id="validate", dataset=path, rules=RULES, fail_on_error=False
    ).execute({})
    assert result["status"] == "FAIL (tolerated)"
    assert result["rows_rejected"] == 3


def test_operator_missing_file_raises(tmp_path):
    operator = DataQualityOperator(
        task_id="validate", dataset=str(tmp_path / "missing.csv"), rules=RULES
    )
    with pytest.raises(DataQualityError, match="could not read dataset"):
        operator.execute({})


def test_operator_rejects_unknown_rule_and_empty_dataset():
    with pytest.raises(ConfigError, match="unknown data-quality rule"):
        DataQualityOperator(
            task_id="validate", dataset="data/orders.csv", rules={"bogus": []}
        )
    with pytest.raises(ConfigError, match="non-empty dataset"):
        DataQualityOperator(task_id="validate", dataset="", rules=RULES)


def test_operator_accepts_typed_rules(tmp_path):
    path = _write_orders(tmp_path, generate_orders(rows=5, include_bad_rows=False))
    result = DataQualityOperator(
        task_id="validate",
        dataset=path,
        rules=load_config().data_quality,
    ).execute({})
    assert result["status"] == "PASS"


def test_sensor_pokes_true_when_partition_exists(tmp_path):
    (tmp_path / "2026-11-22.csv").write_text("x")
    sensor = PartitionSensor(task_id="wait", base_path=str(tmp_path))
    assert sensor.poke({"ds": "2026-11-22"}) is True
    assert sensor.poke({"ds": "2026-11-23"}) is False


def test_sensor_supports_ds_nodash_template(tmp_path):
    (tmp_path / "20261122.csv").write_text("x")
    sensor = PartitionSensor(
        task_id="wait", base_path=str(tmp_path), partition_template="{ds_nodash}.csv"
    )
    assert sensor.poke({"ds": "2026-11-22"}) is True


def test_sensor_rejects_bad_template_and_missing_ds(tmp_path):
    with pytest.raises(ConfigError, match="must contain"):
        PartitionSensor(
            task_id="wait", base_path=str(tmp_path), partition_template="fixed.csv"
        )
    sensor = PartitionSensor(task_id="wait", base_path=str(tmp_path))
    with pytest.raises(ConfigError, match="'ds' in the context"):
        sensor.poke({})
