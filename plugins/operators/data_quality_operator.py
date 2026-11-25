"""Custom operator: run rule checks against a dataset and fail the task
when a rule breaks, so the load never runs on bad data.

Rules are config, not code: not-null columns, unique columns, and
numeric ranges, all declared on the task (or read from
``config/pipelines.yaml``). The rule engine is the same one the local
runner uses (``src/validation.py``), so the checks stay in sync.

The operator reads the staged dataset from a CSV file. ``dataset`` is
the file path (absolute, or relative to the project root); in a real
deployment that path is the staging location the extract task wrote.
Airflow is an optional import here: when it is not installed, a small
stub base class stands in so the operator can still be imported and
unit-tested locally.
"""

from __future__ import annotations

import logging
from pathlib import Path

try:  # Airflow is a deployment dependency, not a local-test one.
    from airflow.models import BaseOperator
except ImportError:  # pragma: no cover - exercised when Airflow is absent

    class BaseOperator:  # type: ignore[no-redef]
        """Minimal stand-in for airflow.models.BaseOperator."""

        template_fields: tuple[str, ...] = ()

        def __init__(self, task_id: str, **kwargs: object) -> None:
            self.task_id = task_id
            self.log = logging.getLogger(f"{__name__}.{task_id}")
            self._kwargs = kwargs

        def execute(self, context: dict) -> dict:  # noqa: D102
            raise NotImplementedError


from src.config import DataQualityRules, validate_rules
from src.exceptions import ConfigError, DataQualityError, PipelineError
from src.storage import read_csv
from src.validation import validate_rows

logger = logging.getLogger(__name__)


class DataQualityOperator(BaseOperator):
    """Validate a CSV dataset against not-null, uniqueness, and range rules.

    Args:
        dataset: Path to the staged CSV file to validate. Relative
            paths resolve against the project root.
        rules: Rule mapping, e.g. ``{"not_null": ["order_id"],
            "unique": ["order_id"], "range": {"amount": [0, 10000]}}``.
        fail_on_error: When False, log failures and return the result
            instead of raising. Defaults to True: a rule failure fails
            the task and triggers Airflow's retry.
    """

    template_fields = ("dataset",)

    def __init__(
        self,
        dataset: str,
        rules: dict | DataQualityRules,
        fail_on_error: bool = True,
        **kwargs: object,
    ) -> None:
        super().__init__(**kwargs)
        if not dataset or not isinstance(dataset, str):
            raise ConfigError("DataQualityOperator requires a non-empty dataset path")
        self.dataset = dataset
        self.fail_on_error = fail_on_error
        self.rules = rules if isinstance(rules, DataQualityRules) else validate_rules(rules)
        self.log.info("DataQualityOperator configured for %s", dataset)

    def _resolve_path(self) -> Path:
        path = Path(self.dataset)
        if path.is_absolute():
            return path
        project_root = Path(__file__).resolve().parent.parent.parent
        return project_root / path

    def _read_dataset(self) -> list[dict[str, str]]:
        path = self._resolve_path()
        try:
            return read_csv(path)
        except PipelineError as exc:
            raise DataQualityError(f"could not read dataset {self.dataset}: {exc}") from exc

    def execute(self, context: dict) -> dict:
        rows = self._read_dataset()
        result = validate_rows(rows, self.rules)
        self.log.info(
            "data quality for %s: %d rows in, %d clean, %d rejected %s",
            self.dataset,
            result.rows_in,
            result.rows_clean,
            result.rows_rejected,
            result.failures_by_rule,
        )
        if result.failures and self.fail_on_error:
            raise DataQualityError(
                f"data quality failed for {self.dataset}: "
                + "; ".join(
                    f"{failure.rule} on {failure.order_id} ({failure.detail})"
                    for failure in result.failures
                ),
                rows_in=result.rows_in,
                rows_rejected=result.rows_rejected,
                failures_by_rule=result.failures_by_rule,
            )
        status = "PASS" if result.passed else "FAIL (tolerated)"
        self.log.info("data quality %s for %s", status, self.dataset)
        return {
            "dataset": self.dataset,
            "rows": result.rows_in,
            "rows_clean": result.rows_clean,
            "rows_rejected": result.rows_rejected,
            "failures_by_rule": result.failures_by_rule,
            "status": status,
        }
