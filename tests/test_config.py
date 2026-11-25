"""Validate config/pipelines.yaml: every DAG has a schedule, retries,
an SLA, and catchup off; rules and schema are well-formed."""

from pathlib import Path

import pytest
import yaml

from src.config import ConfigError, load_config
from src.schema import ORDER_COLUMNS

ROOT = Path(__file__).resolve().parent.parent


def test_config_loads_and_covers_both_dags():
    config = yaml.safe_load((ROOT / "config" / "pipelines.yaml").read_text())
    dags = config["dags"]
    assert set(dags) == {"daily_orders_pipeline", "weekly_aggregate_pipeline"}
    for dag_id, settings in dags.items():
        assert settings["schedule"], f"{dag_id} missing schedule"
        assert settings["retries"] >= 1, f"{dag_id} must retry"
        assert settings["sla_minutes"] > 0, f"{dag_id} missing SLA"
        assert settings["catchup"] is False, f"{dag_id} must not catch up"
        assert settings["owner"] == "data-engineering"


def test_data_quality_rules_configured():
    config = yaml.safe_load((ROOT / "config" / "pipelines.yaml").read_text())
    rules = config["data_quality"]
    assert "order_id" in rules["not_null"]
    assert "order_id" in rules["unique"]
    assert rules["range"]["amount"] == [0, 10000]


def test_typed_config_matches_yaml():
    config = load_config()
    assert set(config.dags) == {"daily_orders_pipeline", "weekly_aggregate_pipeline"}
    daily = config.dag("daily_orders_pipeline")
    assert daily.retries == 2
    assert daily.retry_delay_minutes == 5
    assert daily.sla_minutes == 30
    assert daily.catchup is False
    assert daily.max_active_runs == 1
    weekly = config.dag("weekly_aggregate_pipeline")
    assert weekly.schedule == "0 6 * * 1"
    assert weekly.sla_minutes == 120
    assert config.data_quality.not_null == ("order_id", "customer_id", "amount")
    assert config.data_quality.unique == ("order_id",)
    assert config.data_quality.range == {"amount": (0.0, 10000.0)}
    assert config.weekly_wait_days == 7
    assert config.start_date == "2026-11-01"


def test_schema_columns_in_config_match_code():
    config = load_config()
    assert config.schema_columns == ORDER_COLUMNS


def test_unknown_dag_raises():
    with pytest.raises(ConfigError):
        load_config().dag("no_such_dag")


def test_missing_config_file_raises(tmp_path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "does-not-exist.yaml")


def test_invalid_yaml_raises(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("dags: [unclosed\n")
    with pytest.raises(ConfigError):
        load_config(bad)


def _write_config(tmp_path, text):
    path = tmp_path / "pipelines.yaml"
    path.write_text(text)
    return path


def test_unknown_rule_key_raises(tmp_path):
    path = _write_config(
        tmp_path,
        "dags:\n  daily_orders_pipeline:\n    schedule: daily\n    owner: data-engineering\n"
        "data_quality:\n  not_null: [order_id]\n  typo_rule: [x]\n"
        "schema:\n  columns: [order_id]\n",
    )
    with pytest.raises(ConfigError, match="unknown data-quality rule"):
        load_config(path)


def test_reversed_range_raises(tmp_path):
    path = _write_config(
        tmp_path,
        "dags:\n  daily_orders_pipeline:\n    schedule: daily\n    owner: data-engineering\n"
        "data_quality:\n  range:\n    amount: [10000, 0]\n"
        "schema:\n  columns: [order_id]\n",
    )
    with pytest.raises(ConfigError, match="low .* > high"):
        load_config(path)


def test_zero_sla_rejected(tmp_path):
    path = _write_config(
        tmp_path,
        "dags:\n  daily_orders_pipeline:\n    schedule: daily\n    owner: data-engineering\n"
        "    retries: 0\n    sla_minutes: 0\n"
        "data_quality: {}\n"
        "schema:\n  columns: [order_id]\n",
    )
    with pytest.raises(ConfigError):
        load_config(path)
