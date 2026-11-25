"""Import the DAG files with stubbed Airflow modules and check they build.

Airflow is not installed here, so ``airflow`` and its submodules are
replaced with small stubs that record the DAG, tasks, and sensors the
files define. This catches real wiring mistakes (a bad kwarg, a wrong
import, a sensor count that does not match config) that AST checks
cannot see, without needing a scheduler.
"""

import importlib.util
import logging
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
DAGS = ROOT / "dags"
PLUGINS = ROOT / "plugins"


class _Task:
    """Stand-in for an operator/sensor: records its kwargs, chains with >>."""

    created: list = []

    def __init__(self, task_id, **kwargs):
        self.task_id = task_id
        self.kwargs = kwargs
        self.log = logging.getLogger(f"stub.{task_id}")
        type(self).created.append(self)

    def __rshift__(self, other):
        return other

    def __rrshift__(self, other):
        return self


class _DAG:
    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _TaskGroup:
    def __init__(self, group_id):
        self.group_id = group_id

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __rshift__(self, other):
        return other

    def __rrshift__(self, other):
        return self


def _install_airflow_stub():
    """Install stub airflow modules; return the names added to sys.modules."""
    airflow = types.ModuleType("airflow")
    airflow.DAG = _DAG
    models = types.ModuleType("airflow.models")
    models.BaseOperator = _Task
    sensors_base = types.ModuleType("airflow.sensors.base")
    sensors_base.BaseSensorOperator = _Task
    sensors = types.ModuleType("airflow.sensors")
    sensors.__path__ = []
    external_task = types.ModuleType("airflow.sensors.external_task")
    external_task.ExternalTaskSensor = _Task
    operators = types.ModuleType("airflow.operators")
    operators.__path__ = []
    python_op = types.ModuleType("airflow.operators.python")
    python_op.PythonOperator = _Task
    utils = types.ModuleType("airflow.utils")
    utils.__path__ = []
    task_group = types.ModuleType("airflow.utils.task_group")
    task_group.TaskGroup = _TaskGroup
    modules = {
        "airflow": airflow,
        "airflow.models": models,
        "airflow.sensors": sensors,
        "airflow.sensors.base": sensors_base,
        "airflow.sensors.external_task": external_task,
        "airflow.operators": operators,
        "airflow.operators.python": python_op,
        "airflow.utils": utils,
        "airflow.utils.task_group": task_group,
    }
    saved = {name: sys.modules.get(name) for name in modules}
    sys.modules.update(modules)
    return saved


def _restore_airflow_stub(saved):
    for name, module in saved.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


def _load_dag_module(name):
    spec = importlib.util.spec_from_file_location(name, DAGS / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def airflow_stub(monkeypatch):
    saved = _install_airflow_stub()
    monkeypatch.syspath_prepend(str(DAGS))
    monkeypatch.syspath_prepend(str(PLUGINS))
    monkeypatch.syspath_prepend(str(ROOT))
    _Task.created = []
    yield
    _restore_airflow_stub(saved)
    for name in ("daily_orders_pipeline", "weekly_aggregate_pipeline"):
        sys.modules.pop(name, None)


def test_daily_dag_builds_with_real_tasks(airflow_stub):
    module = _load_dag_module("daily_orders_pipeline")
    dag = module.dag
    assert dag.dag_id == "daily_orders_pipeline"
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert "schedule" in dag.kwargs
    assert "schedule_interval" not in dag.kwargs
    assert dag.kwargs["default_args"]["retries"] == 2
    task_ids = [t.task_id for t in _Task.created]
    for expected in ("extract", "validate", "clean", "enrich", "model", "load", "notify"):
        assert expected in task_ids, f"missing task {expected}"
    validate = next(t for t in _Task.created if t.task_id == "validate")
    assert validate.dataset == "data/staging_orders.csv"
    assert validate.rules.to_dict()["range"] == {"amount": [0, 10000]}


def test_weekly_dag_builds_seven_sensors(airflow_stub):
    module = _load_dag_module("weekly_aggregate_pipeline")
    dag = module.dag
    assert dag.dag_id == "weekly_aggregate_pipeline"
    assert dag.kwargs["catchup"] is False
    sensors = [
        t
        for t in _Task.created
        if t.task_id.startswith("wait_for_daily_day_")
    ]
    assert len(sensors) == 7
    assert {s.kwargs["external_dag_id"] for s in sensors} == {"daily_orders_pipeline"}
    assert {s.kwargs["external_task_id"] for s in sensors} == {"load"}
    assert {s.kwargs["mode"] for s in sensors} == {"reschedule"}
    deltas = sorted(s.kwargs["execution_delta"].days for s in sensors)
    assert deltas == [1, 2, 3, 4, 5, 6, 7]
    assert "aggregate" in [t.task_id for t in _Task.created]
