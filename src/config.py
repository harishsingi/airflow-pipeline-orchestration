"""Configuration loading: one typed source of truth for every DAG.

``config/pipelines.yaml`` holds the schedule, retry policy, SLA, owner,
data-quality rules, and expected schema. The DAGs (via ``dags/common.py``)
and the local runner both read it through :func:`load_config`, so a policy
change in the YAML is the only change needed. Nothing in this module
imports Airflow, so it is importable (and testable) without a scheduler.

Resolution order for the config file: explicit ``path`` argument, then
the ``PIPELINE_CONFIG`` environment variable, then
``<project root>/config/pipelines.yaml``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from src.exceptions import ConfigError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pipelines.yaml"
CONFIG_ENV_VAR = "PIPELINE_CONFIG"

DEFAULT_OWNER = "data-engineering"
DEFAULT_RETRIES = 2
DEFAULT_RETRY_DELAY_MINUTES = 5
DEFAULT_SLA_MINUTES = 30
DEFAULT_MAX_ACTIVE_RUNS = 1
DEFAULT_START_DATE = "2026-11-01"


@dataclass(frozen=True)
class DataQualityRules:
    """Typed data-quality rules, validated at load time."""

    not_null: tuple[str, ...] = ()
    unique: tuple[str, ...] = ()
    range: dict[str, tuple[float, float]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Plain-dict form, as the Airflow operator expects it."""
        return {
            "not_null": list(self.not_null),
            "unique": list(self.unique),
            "range": {k: list(v) for k, v in self.range.items()},
        }


@dataclass(frozen=True)
class DagSettings:
    """Schedule / retry / SLA policy for a single DAG."""

    dag_id: str
    schedule: str
    owner: str = DEFAULT_OWNER
    retries: int = DEFAULT_RETRIES
    retry_delay_minutes: int = DEFAULT_RETRY_DELAY_MINUTES
    sla_minutes: int = DEFAULT_SLA_MINUTES
    catchup: bool = False
    max_active_runs: int = DEFAULT_MAX_ACTIVE_RUNS


@dataclass(frozen=True)
class PipelineConfig:
    """Everything the DAGs and the local runner need from the YAML."""

    dags: dict[str, DagSettings]
    data_quality: DataQualityRules
    schema_columns: tuple[str, ...]
    start_date: str = DEFAULT_START_DATE
    warehouse_table: str = "warehouse/orders"
    staging_dataset: str = "staging/orders"
    weekly_wait_days: int = 7

    def dag(self, dag_id: str) -> DagSettings:
        """Return settings for ``dag_id`` or raise :class:`ConfigError`."""
        try:
            return self.dags[dag_id]
        except KeyError as exc:
            raise ConfigError(f"no config entry for DAG {dag_id!r}") from exc


def resolve_config_path(path: str | Path | None = None) -> Path:
    """Resolve which config file to load, honouring the env override."""
    if path is not None:
        return Path(path)
    env_path = os.environ.get(CONFIG_ENV_VAR)
    if env_path:
        return Path(env_path)
    return DEFAULT_CONFIG_PATH


def validate_rules(raw: Any) -> DataQualityRules:
    """Validate a raw ``data_quality`` mapping into typed rules.

    Raises :class:`ConfigError` on unknown rule keys, non-list columns,
    or a range whose lower bound exceeds its upper bound. Unknown keys
    fail loudly: silently ignoring a misspelled rule would let bad data
    through while looking protected.
    """
    if not isinstance(raw, dict):
        raise ConfigError("data_quality must be a mapping")
    allowed = {"not_null", "unique", "range"}
    unknown = set(raw) - allowed
    if unknown:
        raise ConfigError(f"unknown data-quality rule(s): {sorted(unknown)}")
    not_null = _column_list(raw.get("not_null", []), "not_null")
    unique = _column_list(raw.get("unique", []), "unique")
    ranges: dict[str, tuple[float, float]] = {}
    raw_range = raw.get("range", {})
    if not isinstance(raw_range, dict):
        raise ConfigError("data_quality.range must be a mapping")
    for column, bounds in raw_range.items():
        if not isinstance(bounds, (list, tuple)) or len(bounds) != 2:
            raise ConfigError(
                f"data_quality.range[{column!r}] must be [low, high]"
            )
        try:
            low, high = float(bounds[0]), float(bounds[1])
        except (TypeError, ValueError) as exc:
            raise ConfigError(
                f"data_quality.range[{column!r}] bounds must be numbers"
            ) from exc
        if low > high:
            raise ConfigError(
                f"data_quality.range[{column!r}] has low {low} > high {high}"
            )
        ranges[str(column)] = (low, high)
    return DataQualityRules(not_null=not_null, unique=unique, range=ranges)


def _column_list(raw: Any, name: str) -> tuple[str, ...]:
    if not isinstance(raw, list) or not all(isinstance(c, str) for c in raw):
        raise ConfigError(f"data_quality.{name} must be a list of column names")
    return tuple(raw)


def _dag_settings(dag_id: str, raw: Any) -> DagSettings:
    if not isinstance(raw, dict):
        raise ConfigError(f"dags.{dag_id} must be a mapping")
    for key in ("schedule", "owner"):
        if not raw.get(key):
            raise ConfigError(f"dags.{dag_id} is missing {key!r}")
    retries = raw.get("retries", DEFAULT_RETRIES)
    sla = raw.get("sla_minutes", DEFAULT_SLA_MINUTES)
    if not isinstance(retries, int) or retries < 0:
        raise ConfigError(f"dags.{dag_id}.retries must be an integer >= 0")
    if not isinstance(sla, int) or sla <= 0:
        raise ConfigError(f"dags.{dag_id}.sla_minutes must be an integer > 0")
    catchup = raw.get("catchup", False)
    if not isinstance(catchup, bool):
        raise ConfigError(f"dags.{dag_id}.catchup must be true/false")
    return DagSettings(
        dag_id=dag_id,
        schedule=str(raw["schedule"]),
        owner=str(raw["owner"]),
        retries=retries,
        retry_delay_minutes=int(raw.get("retry_delay_minutes", DEFAULT_RETRY_DELAY_MINUTES)),
        sla_minutes=sla,
        catchup=catchup,
        max_active_runs=int(raw.get("max_active_runs", DEFAULT_MAX_ACTIVE_RUNS)),
    )


def load_config(path: str | Path | None = None) -> PipelineConfig:
    """Load and validate ``config/pipelines.yaml`` (or ``path``)."""
    config_path = resolve_config_path(path)
    if not config_path.is_file():
        raise ConfigError(f"config file not found: {config_path}")
    try:
        raw = yaml.safe_load(config_path.read_text())
    except yaml.YAMLError as exc:
        raise ConfigError(f"config file is not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("config file must contain a mapping")
    raw_dags = raw.get("dags")
    if not isinstance(raw_dags, dict) or not raw_dags:
        raise ConfigError("config file must define at least one DAG under 'dags'")
    dags = {dag_id: _dag_settings(dag_id, settings) for dag_id, settings in raw_dags.items()}
    rules = validate_rules(raw.get("data_quality", {}))
    columns = raw.get("schema", {}).get("columns", []) if isinstance(raw.get("schema"), dict) else []
    if not columns or not all(isinstance(c, str) for c in columns):
        raise ConfigError("config file must list schema.columns as column names")
    wait_days = raw.get("weekly_wait_days", 7)
    if not isinstance(wait_days, int) or wait_days < 1:
        raise ConfigError("weekly_wait_days must be an integer >= 1")
    return PipelineConfig(
        dags=dags,
        data_quality=rules,
        schema_columns=tuple(columns),
        start_date=str(raw.get("start_date", DEFAULT_START_DATE)),
        warehouse_table=str(raw.get("warehouse_table", "warehouse/orders")),
        staging_dataset=str(raw.get("staging_dataset", "staging/orders")),
        weekly_wait_days=wait_days,
    )
