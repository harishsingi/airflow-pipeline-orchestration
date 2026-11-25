"""Sensor: wait until a dated partition file exists before proceeding.

Used when a DAG depends on a file landing (an upstream extract, a
vendor drop) rather than on another DAG's task. Poke mode keeps the
check cheap; reschedule mode frees the worker slot between pokes.

Airflow is an optional import here: when it is not installed, a small
stub base class stands in so the sensor can still be imported and
unit-tested locally.
"""

from __future__ import annotations

import logging
from pathlib import Path

try:  # Airflow is a deployment dependency, not a local-test one.
    from airflow.sensors.base import BaseSensorOperator
except ImportError:  # pragma: no cover - exercised when Airflow is absent

    class BaseSensorOperator:  # type: ignore[no-redef]
        """Minimal stand-in for airflow.sensors.base.BaseSensorOperator."""

        template_fields: tuple[str, ...] = ()

        def __init__(self, task_id: str, **kwargs: object) -> None:
            self.task_id = task_id
            self.log = logging.getLogger(f"{__name__}.{task_id}")
            self._kwargs = kwargs

        def poke(self, context: dict) -> bool:  # noqa: D102
            raise NotImplementedError


from src.exceptions import ConfigError

logger = logging.getLogger(__name__)


class PartitionSensor(BaseSensorOperator):
    """Succeed when the partition file for the run date exists.

    Args:
        base_path: Directory holding the partition files.
        partition_template: File name template, rendered with the
            Airflow context (``{ds}`` is the run date as YYYY-MM-DD,
            ``{ds_nodash}`` as YYYYMMDD). Defaults to ``"{ds}.csv"``.
    """

    template_fields = ("base_path", "partition_template")

    def __init__(
        self,
        base_path: str,
        partition_template: str = "{ds}.csv",
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)
        if not base_path or not isinstance(base_path, str):
            raise ConfigError("PartitionSensor requires a non-empty base_path")
        if "{ds}" not in partition_template and "{ds_nodash}" not in partition_template:
            raise ConfigError(
                "partition_template must contain {ds} or {ds_nodash}, "
                f"got {partition_template!r}"
            )
        self.base_path = base_path
        self.partition_template = partition_template

    def partition_path(self, context: dict) -> Path:
        """Resolve the expected partition file for this run's context."""
        ds = context.get("ds")
        if not ds:
            raise ConfigError("PartitionSensor poke needs 'ds' in the context")
        rendered = self.partition_template.format(
            ds=ds, ds_nodash=str(ds).replace("-", "")
        )
        return Path(self.base_path) / rendered

    def poke(self, context: dict) -> bool:
        path = self.partition_path(context)
        exists = path.is_file()
        self.log.info(
            "checking partition %s: %s", path, "found" if exists else "waiting"
        )
        return exists
