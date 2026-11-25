"""Data-quality rule engine.

Pure functions, no Airflow and no I/O: the same engine backs the
custom ``DataQualityOperator`` (inside the DAG) and the local runner
(``src/run_local.py``), so the checks cannot drift between the two.

Rules, all declared in config, not code:

* ``not_null`` -- a column must not be null/blank.
* ``unique`` -- a column's values must not repeat. Only the *later*
  occurrence of a duplicated value is rejected; the first occurrence
  is kept, so a duplicate never deletes the good row.
* ``range`` -- a numeric column must fall inside ``[low, high]``
  (inclusive). A non-numeric value is a failure, not a crash.

Idempotency note: validation is a pure read of its input rows. Running
it twice over the same input returns the same result, and it never
mutates the rows it checks.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Mapping, Sequence

from src.config import DataQualityRules
from src.exceptions import ConfigError
from src.schema import is_null, parse_amount

logger = logging.getLogger(__name__)

Row = Mapping[str, object]


@dataclass(frozen=True)
class RuleFailure:
    """One rule broken by one row."""

    order_id: str
    rule: str
    detail: str


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of validating a batch of rows, with metrics."""

    rows_in: int
    bad_indices: frozenset[int] = frozenset()
    failures: tuple[RuleFailure, ...] = ()

    @property
    def rows_rejected(self) -> int:
        return len(self.bad_indices)

    @property
    def rows_clean(self) -> int:
        return self.rows_in - self.rows_rejected

    @property
    def failures_by_rule(self) -> dict[str, int]:
        return dict(Counter(f.rule for f in self.failures))

    @property
    def passed(self) -> bool:
        return not self.failures

    def rejected_order_ids(self) -> list[str]:
        return [f.order_id for f in self.failures]


def _order_id(row: Row, index: int) -> str:
    value = row.get("order_id")
    return str(value) if value not in (None, "") else f"<row {index}>"


def validate_rows(
    rows: Sequence[Row],
    rules: DataQualityRules,
) -> ValidationResult:
    """Run all configured rules over ``rows`` and return the result.

    Never raises on bad data: every broken rule is recorded as a
    :class:`RuleFailure` and the offending row index is marked bad.
    Raises :class:`ConfigError` only when the rules themselves reference
    a column that is absent from every row (a config mistake, not a
    data mistake).
    """
    rows = list(rows)
    if rows:
        present = set(rows[0].keys())
        for column in (*rules.not_null, *rules.unique, *rules.range):
            if column not in present:
                raise ConfigError(
                    f"data-quality rule references unknown column {column!r}"
                )
    failures: list[RuleFailure] = []
    bad_indices: set[int] = set()

    for column in rules.not_null:
        for index, row in enumerate(rows):
            if is_null(row.get(column)):
                failures.append(
                    RuleFailure(
                        order_id=_order_id(row, index),
                        rule=f"not_null:{column}",
                        detail=f"{column} is null or blank",
                    )
                )
                bad_indices.add(index)

    for column in rules.unique:
        seen: set[object] = set()
        for index, row in enumerate(rows):
            value = row.get(column)
            if value in seen:
                failures.append(
                    RuleFailure(
                        order_id=_order_id(row, index),
                        rule=f"unique:{column}",
                        detail=f"duplicate {column}={value!r} (first occurrence kept)",
                    )
                )
                bad_indices.add(index)
            else:
                seen.add(value)

    for column, (low, high) in rules.range.items():
        for index, row in enumerate(rows):
            amount = parse_amount(row.get(column))
            if amount is None:
                failures.append(
                    RuleFailure(
                        order_id=_order_id(row, index),
                        rule=f"range:{column}",
                        detail=f"{column}={row.get(column)!r} is not a number",
                    )
                )
                bad_indices.add(index)
            elif not low <= amount <= high:
                failures.append(
                    RuleFailure(
                        order_id=_order_id(row, index),
                        rule=f"range:{column}",
                        detail=f"{column}={amount} outside [{low}, {high}]",
                    )
                )
                bad_indices.add(index)

    result = ValidationResult(
        rows_in=len(rows),
        bad_indices=frozenset(bad_indices),
        failures=tuple(failures),
    )
    logger.info(
        "validation: %d rows in, %d clean, %d rejected %s",
        result.rows_in,
        result.rows_clean,
        result.rows_rejected,
        result.failures_by_rule,
    )
    return result


def split_clean_and_quarantine(
    rows: Sequence[dict[str, str]],
    result: ValidationResult,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Split rows into clean and quarantined lists.

    Quarantined rows are never passed downstream; they are written to
    the quarantine file by the caller for inspection and reprocessing.
    Input rows are copied, never mutated.
    """
    clean = [dict(row) for i, row in enumerate(rows) if i not in result.bad_indices]
    quarantined = [dict(row) for i, row in enumerate(rows) if i in result.bad_indices]
    return clean, quarantined
