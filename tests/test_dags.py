"""Static validation of the DAG definitions.

Airflow is not installed in this environment, so the DAG files are checked
by parsing them (AST) instead of importing them: structure, retries,
catchup, and dependencies are all verifiable without a scheduler. The
real stage logic is covered by the unit tests in test_pipeline.py and
test_operator_sensor.py.
"""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DAGS = ROOT / "dags"

BANNED_TERMS = [
    "bmw",
    "global logic",
    "globallogic",
    "greenville",
    "illinois",
    "jntu",
    "texas tech",
]


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


def _kw_str(call, name):
    """String value of a DAG kwarg, whether literal or a named constant."""
    node = _kw(call, name)
    if node is None:
        return None
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return node.id
    return None


def _kw_bool(call, name):
    node = _kw(call, name)
    if isinstance(node, ast.Constant):
        return node.value
    return None


def test_daily_dag_structure():
    tree = _parse("daily_orders_pipeline.py")
    dag = _dag_call(tree)
    assert _kw_str(dag, "dag_id") in ("daily_orders_pipeline", "DAG_ID")
    assert _kw_bool(dag, "catchup") is False
    assert _kw(dag, "max_active_runs").value == 1
    # Modern Airflow kwarg: schedule=, not the deprecated schedule_interval.
    assert _kw(dag, "schedule") is not None
    assert _kw(dag, "schedule_interval") is None
    source = (DAGS / "daily_orders_pipeline.py").read_text()
    for task in ["extract", "validate", "load", "notify"]:
        assert f'task_id="{task}"' in source, f"missing task {task}"
    assert 'group_id="transform"' in source
    assert "DataQualityOperator" in source
    # Dependency chain: extract >> validate >> transform >> load >> notify
    assert "extract_task >> validate_task >> transform_group >> load_task >> notify_task" in source
    # Stage logic is delegated to src/, not stubbed in the DAG file.
    assert "stage_extract" in source
    assert "from src.config import load_config" in source


def test_daily_dag_has_no_hardcoded_counts_or_prints():
    source = (DAGS / "daily_orders_pipeline.py").read_text()
    assert "print(" not in source, "DAG tasks must log, not print"
    for hardcoded in ['"staged_records": 120', '"clean_records": 118', '"loaded_records": 118']:
        assert hardcoded not in source, f"DAG still hardcodes {hardcoded}"


def test_weekly_dag_waits_for_the_whole_week():
    tree = _parse("weekly_aggregate_pipeline.py")
    dag = _dag_call(tree)
    assert _kw_str(dag, "dag_id") in ("weekly_aggregate_pipeline", "DAG_ID")
    assert _kw_bool(dag, "catchup") is False
    assert _kw(dag, "schedule") is not None
    assert _kw(dag, "schedule_interval") is None
    sensors = _calls(tree, "ExternalTaskSensor")
    assert len(sensors) == 1, "one sensor call site, built in a loop over the week"
    sensor = sensors[0]
    assert _kw(sensor, "external_dag_id").value == "daily_orders_pipeline"
    assert _kw(sensor, "external_task_id").value == "load"
    assert _kw(sensor, "mode").value == "reschedule"
    source = (DAGS / "weekly_aggregate_pipeline.py").read_text()
    assert 'group_id="wait_for_daily_week"' in source
    # One sensor per day of the week, driven by config (default 7).
    assert "range(1, WAIT_DAYS + 1)" in source
    assert "WAIT_DAYS = CONFIG.weekly_wait_days" in source
    assert "wait_group >> aggregate_task" in source


def test_common_defaults_carry_retries_and_sla():
    source = (DAGS / "common.py").read_text()
    assert '"retries": 2' in source
    assert '"retry_delay": timedelta(minutes=5)' in source
    assert '"sla": timedelta(minutes=30)' in source
    assert "depends_on_past" in source
    assert "def default_args_for" in source, "per-DAG defaults must come from config"
    assert "def on_failure" in source
    assert "print(" not in source, "common.py must log, not print"


def test_custom_operator_uses_shared_rule_engine():
    source = (ROOT / "plugins" / "operators" / "data_quality_operator.py").read_text()
    assert "class DataQualityOperator" in source
    for rule in ["not_null", "unique", "range"]:
        assert rule in source, f"operator missing {rule} rule"
    assert "validate_rows" in source, "operator must use the shared rule engine"
    assert "raise DataQualityError" in source, "operator must fail the task on bad data"
    assert "return []" not in source, "operator must actually read its dataset"
    assert "template_fields" in source


def test_partition_sensor_exists():
    source = (ROOT / "plugins" / "sensors" / "partition_sensor.py").read_text()
    assert "class PartitionSensor" in source
    assert "def poke" in source
    assert "template_fields" in source
    assert "is_file()" in source


def test_no_company_names_anywhere():
    for path in ROOT.rglob("*"):
        # The test file itself names the banned terms; skip it.
        if path.name == "test_dags.py":
            continue
        if ".git/" in str(path):
            continue
        if path.is_file() and path.suffix in {".py", ".md", ".yaml", ".toml", ".txt"}:
            text = path.read_text(errors="ignore").lower()
            for term in BANNED_TERMS:
                assert term not in text, f"{path} contains banned term {term!r}"


def test_no_hardcoded_credentials_or_absolute_paths():
    # Only production code is scanned: this test file itself names the
    # marker strings, so tests/ is excluded.
    scanned = [
        p
        for p in list(ROOT.rglob("*.py")) + list(ROOT.rglob("*.yaml"))
        if "tests" not in p.parts
    ]
    for path in scanned:
        if ".git/" in str(path):
            continue
        text = path.read_text(errors="ignore")
        lowered = text.lower()
        for marker in ["password =", "api_key =", "secret =", "aws_secret"]:
            assert marker not in lowered, f"{path} looks like a hardcoded credential"
        assert "/home/" not in text, f"{path} contains a hardcoded home path"
