"""Generate synthetic order data for local runs.

All values are invented; no real customer, order, or amount data is
used. A few deliberately bad rows (missing customer, duplicate id,
out-of-range amount) are included by default so the validation step
has something to catch and quarantine.

Determinism: a dedicated ``random.Random(seed)`` instance is used, so
the same seed always produces the same file and the global random
state is never touched.
"""

from __future__ import annotations

import argparse
import logging
import random
from datetime import datetime
from pathlib import Path

from src.exceptions import PipelineError
from src.schema import DATE_FORMAT, ORDER_COLUMNS
from src.storage import write_csv_atomic

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "orders.csv"
DEFAULT_ROWS = 118
DEFAULT_RUN_DATE = "2026-11-22"
DEFAULT_SEED = 42

REGIONS: tuple[str, ...] = ("north", "south", "east", "west")
CHANNELS: tuple[str, ...] = ("web", "mobile", "store")
MIN_AMOUNT = 5.0
MAX_AMOUNT = 9500.0
OUT_OF_RANGE_AMOUNT = 25000.0


def _validate_date(value: str) -> str:
    try:
        datetime.strptime(value, DATE_FORMAT)
    except ValueError as exc:
        raise PipelineError(
            f"run date must be YYYY-MM-DD, got {value!r}"
        ) from exc
    return value


def generate_orders(
    rows: int = DEFAULT_ROWS,
    run_date: str = DEFAULT_RUN_DATE,
    seed: int = DEFAULT_SEED,
    include_bad_rows: bool = True,
) -> list[dict[str, str]]:
    """Generate ``rows`` good synthetic orders plus 3 bad ones by default.

    Raises :class:`PipelineError` for a non-positive row count or an
    invalid date. The bad rows are fixed and documented: a missing
    ``customer_id``, a duplicate ``order_id`` (of the first row), and
    an ``amount`` above the configured range.
    """
    if rows < 1:
        raise PipelineError(f"rows must be >= 1, got {rows}")
    _validate_date(run_date)
    rng = random.Random(seed)
    orders: list[dict[str, str]] = []
    for i in range(1, rows + 1):
        orders.append(
            {
                "order_id": f"ORD-{10000 + i}",
                "customer_id": f"CUST-{rng.randint(100, 999)}",
                "amount": f"{rng.uniform(MIN_AMOUNT, MAX_AMOUNT):.2f}",
                "region": rng.choice(REGIONS),
                "channel": rng.choice(CHANNELS),
                "order_date": run_date,
            }
        )
    if include_bad_rows:
        orders.append(
            {
                "order_id": f"ORD-{10000 + rows + 1}",
                "customer_id": "",
                "amount": "120.50",
                "region": "north",
                "channel": "web",
                "order_date": run_date,
            }
        )
        orders.append(
            {
                "order_id": "ORD-10001",  # duplicate id (first row kept)
                "customer_id": "CUST-555",
                "amount": "42.00",
                "region": "south",
                "channel": "mobile",
                "order_date": run_date,
            }
        )
        orders.append(
            {
                "order_id": f"ORD-{10000 + rows + 2}",
                "customer_id": "CUST-777",
                "amount": f"{OUT_OF_RANGE_AMOUNT:.2f}",  # out of range
                "region": "east",
                "channel": "store",
                "order_date": run_date,
            }
        )
    return orders


def main(argv: list[str] | None = None) -> int:
    """CLI: ``python -m src.generate_data [--rows N] [--date D] [--seed S] [--out PATH]``."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--date", default=DEFAULT_RUN_DATE)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-bad-rows", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        orders = generate_orders(
            rows=args.rows,
            run_date=args.date,
            seed=args.seed,
            include_bad_rows=not args.no_bad_rows,
        )
    except PipelineError as exc:
        parser.error(str(exc))
    write_csv_atomic(args.out, orders, ORDER_COLUMNS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
