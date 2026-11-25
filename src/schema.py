"""Schema for the synthetic orders data.

The schema is the contract between the generator, the validator, and the
warehouse model. It lives in one place so a column rename cannot drift
between modules, and :func:`validate_schema` fails fast (before any rule
checks) when the input file does not match it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Mapping

from src.exceptions import SchemaError

# Canonical column order for the raw orders file. Also declared in
# config/pipelines.yaml under ``schema.columns``; the two must agree and
# tests check that.
ORDER_COLUMNS: tuple[str, ...] = (
    "order_id",
    "customer_id",
    "amount",
    "region",
    "channel",
    "order_date",
)

REQUIRED_COLUMNS: tuple[str, ...] = ORDER_COLUMNS

# Warehouse model columns, in write order (see src/transform.model_rows).
WAREHOUSE_COLUMNS: tuple[str, ...] = (
    "order_id",
    "customer_id",
    "amount_usd",
    "region",
    "channel",
    "order_date",
)

VALID_REGIONS: frozenset[str] = frozenset({"north", "south", "east", "west"})
VALID_CHANNELS: frozenset[str] = frozenset({"web", "mobile", "store"})

DATE_FORMAT = "%Y-%m-%d"


def validate_schema(
    rows: list[dict[str, str]],
    expected_columns: tuple[str, ...] = ORDER_COLUMNS,
) -> None:
    """Check ``rows`` match the expected schema.

    Raises :class:`SchemaError` when there are no rows at all, when any
    required column is missing from the header, or when a row's
    ``amount`` / ``order_date`` cannot be parsed. Extra columns are
    rejected too: an unexpected column usually means the source changed
    shape and the pipeline should stop, not silently drop data.
    """
    if not rows:
        raise SchemaError("input has no data rows")
    header = set(rows[0].keys())
    missing = [c for c in expected_columns if c not in header]
    if missing:
        raise SchemaError(f"missing required column(s): {missing}")
    extra = sorted(header - set(expected_columns))
    if extra:
        raise SchemaError(f"unexpected column(s): {extra}")
    for index, row in enumerate(rows):
        for column in expected_columns:
            if column not in row:
                raise SchemaError(f"row {index} is missing column {column!r}")
        try:
            float(row["amount"])
        except (TypeError, ValueError) as exc:
            raise SchemaError(
                f"row {index} ({row.get('order_id', '?')}): "
                f"amount {row['amount']!r} is not a number"
            ) from exc
        try:
            datetime.strptime(str(row["order_date"]), DATE_FORMAT)
        except ValueError as exc:
            raise SchemaError(
                f"row {index} ({row.get('order_id', '?')}): "
                f"order_date {row['order_date']!r} is not YYYY-MM-DD"
            ) from exc


def is_null(value: object) -> bool:
    """Null for validation purposes: None, or an empty/blank string."""
    return value is None or (isinstance(value, str) and value.strip() == "")


def parse_amount(value: object) -> float | None:
    """Parse an amount to float, returning None when it is not numeric."""
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def is_valid_row_shape(row: Mapping[str, object]) -> bool:
    """Lightweight shape check used before rule evaluation."""
    return all(column in row for column in REQUIRED_COLUMNS)
