"""Transform steps: clean, enrich, model.

Each step is a pure function over row dicts: same input, same output,
no mutation of the input, no I/O. That is what lets the DAG's
``transform`` TaskGroup call them as separate tasks and lets the local
runner call them in sequence with identical results.

* ``clean_rows`` -- drop rows the validator marked bad.
* ``enrich_rows`` -- normalise region/channel and derive ``amount_usd``.
* ``model_rows`` -- project to the warehouse column order.
"""

from __future__ import annotations

import logging
from typing import Sequence

from src.exceptions import TransformError
from src.schema import (
    VALID_CHANNELS,
    VALID_REGIONS,
    WAREHOUSE_COLUMNS,
    parse_amount,
)

logger = logging.getLogger(__name__)


def clean_rows(
    rows: Sequence[dict[str, str]],
    bad_indices: frozenset[int] | set[int],
) -> list[dict[str, str]]:
    """Return copies of the rows whose index is not in ``bad_indices``."""
    clean = [dict(row) for i, row in enumerate(rows) if i not in bad_indices]
    logger.info("transform.clean: %d of %d rows kept", len(clean), len(rows))
    return clean


def enrich_rows(rows: Sequence[dict[str, str]]) -> list[dict[str, str]]:
    """Normalise fields and derive reporting fields.

    Region and channel are lower-cased/stripped and checked against the
    known sets; an unknown value raises :class:`TransformError` instead
    of silently landing in the warehouse under a typo. ``amount_usd``
    is the amount formatted to two decimals.
    """
    enriched: list[dict[str, str]] = []
    for row in rows:
        amount = parse_amount(row.get("amount"))
        if amount is None:
            raise TransformError(
                f"cannot enrich {row.get('order_id', '?')}: "
                f"amount {row.get('amount')!r} is not a number"
            )
        region = str(row.get("region", "")).strip().lower()
        channel = str(row.get("channel", "")).strip().lower()
        if region not in VALID_REGIONS:
            raise TransformError(
                f"cannot enrich {row.get('order_id', '?')}: "
                f"unknown region {region!r}"
            )
        if channel not in VALID_CHANNELS:
            raise TransformError(
                f"cannot enrich {row.get('order_id', '?')}: "
                f"unknown channel {channel!r}"
            )
        new_row = dict(row)
        new_row["region"] = region
        new_row["channel"] = channel
        new_row["amount_usd"] = f"{amount:.2f}"
        enriched.append(new_row)
    logger.info("transform.enrich: %d rows enriched", len(enriched))
    return enriched


def model_rows(rows: Sequence[dict[str, str]]) -> list[dict[str, str]]:
    """Project enriched rows to the warehouse model column order."""
    modeled: list[dict[str, str]] = []
    for row in rows:
        missing = [c for c in WAREHOUSE_COLUMNS if c not in row]
        if missing:
            raise TransformError(
                f"cannot model {row.get('order_id', '?')}: "
                f"missing column(s) {missing}"
            )
        modeled.append({column: row[column] for column in WAREHOUSE_COLUMNS})
    logger.info("transform.model: %d rows modeled", len(modeled))
    return modeled
