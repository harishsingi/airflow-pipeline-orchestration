"""Validate config/pipelines.yaml: every DAG has a schedule, retries,
an SLA, and catchup off."""

from pathlib import Path

import yaml

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
