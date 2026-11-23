"""Static validation of the DAG definitions.

Airflow is not installed in this environment, so the DAG files are checked
by parsing them (AST) instead of importing them: structure, retries,
catchup, and dependencies are all verifiable without a scheduler.
"""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DAGS = ROOT / "dags"

BANNED_TERMS = ["bmw", "global logic", "globallogic"]


def _parse(name):
    return ast.parse((DAGS / name).read_text())


def _calls(tree, func_name):
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == func_name
    ]


def _dag_call(tree):
    dags = _calls(tree, "DAG")
    assert len(dags) == 1, "each DAG file must define exactly one DAG"
    return dags[0]


def _kw(call, name):
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def test_daily_dag_structure():
    tree = _parse("daily_orders_pipeline.py")
    dag = _dag_call(tree)
    assert _kw(dag, "dag_id").value == "daily_orders_pipeline"
    assert _kw(dag, "catchup").value is False
    assert _kw(dag, "max_active_runs").value == 1
    source = (DAGS / "daily_orders_pipeline.py").read_text()
    for task in ["extract", "validate", "load", "notify"]:
        assert f'task_id="{task}"' in source, f"missing task {task}"
    assert 'group_id="transform"' in source
    assert "DataQualityOperator" in source
    # Dependency chain: extract >> validate >> transform >> load >> notify
    assert "extract_task >> validate_task >> transform_group >> load_task >> notify_task" in source


def test_weekly_dag_waits_for_daily():
    tree = _parse("weekly_aggregate_pipeline.py")
    dag = _dag_call(tree)
    assert _kw(dag, "dag_id").value == "weekly_aggregate_pipeline"
    assert _kw(dag, "catchup").value is False
    sensors = _calls(tree, "ExternalTaskSensor")
    assert len(sensors) == 1
    sensor = sensors[0]
    assert _kw(sensor, "external_dag_id").value == "daily_orders_pipeline"
    assert _kw(sensor, "external_task_id").value == "load"
    assert _kw(sensor, "mode").value == "reschedule"
    source = (DAGS / "weekly_aggregate_pipeline.py").read_text()
    assert "wait_for_daily >> aggregate_task" in source


def test_common_defaults_carry_retries_and_sla():
    tree = _parse("common.py")
    source = (DAGS / "common.py").read_text()
    assert '"retries": 2' in source
    assert '"retry_delay": timedelta(minutes=5)' in source
    assert '"sla": timedelta(minutes=30)' in source
    assert "depends_on_past" in source


def test_custom_operator_covers_all_rule_types():
    source = (ROOT / "plugins" / "operators" / "data_quality_operator.py").read_text()
    assert "class DataQualityOperator" in source
    for rule in ["not_null", "unique", "range"]:
        assert rule in source, f"operator missing {rule} rule"
    assert "raise ValueError" in source, "operator must fail the task on bad data"


def test_partition_sensor_exists():
    source = (ROOT / "plugins" / "sensors" / "partition_sensor.py").read_text()
    assert "class PartitionSensor" in source
    assert "def poke" in source


def test_no_company_names_anywhere():
    for path in ROOT.rglob("*"):
        # The test file itself names the banned terms; skip it.
        if path.name == "test_dags.py":
            continue
        if path.is_file() and path.suffix in {".py", ".md", ".yaml", ".toml", ".txt"}:
            text = path.read_text(errors="ignore").lower()
            for term in BANNED_TERMS:
                assert term not in text, f"{path} contains banned term {term!r}"
