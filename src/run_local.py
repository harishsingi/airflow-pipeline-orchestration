"""Local runner: execute the daily pipeline in plain Python.

Mirrors the ``daily_orders_pipeline`` DAG (extract -> validate ->
transform -> load -> notify) by calling the same stage functions in
``src/pipeline.py``, without needing Airflow, so the logic can be
exercised and tested locally. Bad rows are quarantined, never loaded.

Usage::

    python -m src.generate_data
    python -m src.run_local [--date YYYY-MM-DD] [--data-dir data/]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from src.config import PROJECT_ROOT, load_config
from src.exceptions import PipelineError
from src.pipeline import run_daily_pipeline

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_RUN_DATE = "2026-11-22"


def main(argv: list[str] | None = None, data_dir: str | Path | None = None) -> int:
    """Run the daily pipeline locally; return 0 on success, 1 on failure.

    ``data_dir`` is injectable so tests can run against a temporary
    directory instead of the repo's ``data/`` folder.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=DEFAULT_RUN_DATE)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    directory = Path(data_dir) if data_dir is not None else args.data_dir
    source = directory / "orders.csv"
    try:
        config = load_config()
        result = run_daily_pipeline(
            source_path=source,
            output_dir=directory,
            run_date=args.date,
            config=config,
        )
    except PipelineError as exc:
        logger.error("daily pipeline failed: %s", exc)
        return 1
    logger.info(
        "notify: daily_orders_pipeline succeeded, %d rows loaded "
        "(%d quarantined)",
        result.rows_loaded,
        result.rows_rejected,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
