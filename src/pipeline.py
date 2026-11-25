"""Daily pipeline orchestration, shared by the DAG and the local runner.

The DAG's tasks and ``src/run_local.py`` both call the stage functions
here, so the logic exercised locally is the logic Airflow runs. Stages
communicate through files (not giant XCom payloads): each stage reads
its input file and writes its output file for the run date, and the
load overwrites the day's partition, making a re-run for the same date
produce identical output.

Flow: extract -> validate (schema, then rules) -> clean/enrich/model
-> quarantine bad rows -> load clean rows -> report metrics.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.config import DataQualityRules, PipelineConfig, load_config
from src.exceptions import PipelineError
from src.schema import DATE_FORMAT, ORDER_COLUMNS, WAREHOUSE_COLUMNS, validate_schema
from src.storage import read_csv, write_csv_atomic
from src.transform import enrich_rows, model_rows
from src.validation import ValidationResult, split_clean_and_quarantine, validate_rows

logger = logging.getLogger(__name__)

STAGING_FILENAME = "staging_orders.csv"
WAREHOUSE_FILENAME = "warehouse_orders.csv"
QUARANTINE_FILENAME = "quarantine.csv"


@dataclass(frozen=True)
class PipelineRunResult:
    """Metrics for one daily run; also what the DAG logs / returns."""

    run_date: str
    rows_in: int
    rows_clean: int
    rows_rejected: int
    rows_loaded: int
    failures_by_rule: dict[str, int]
    warehouse_path: Path
    quarantine_path: Path | None

    def log_summary(self) -> None:
        logger.info(
            "daily run %s: %d in, %d clean, %d rejected %s, %d loaded to %s",
            self.run_date,
            self.rows_in,
            self.rows_clean,
            self.rows_rejected,
            self.failures_by_rule,
            self.rows_loaded,
            self.warehouse_path,
        )


def partition_paths(output_dir: str | Path, run_date: str) -> dict[str, Path]:
    """Output file paths for one run date under ``output_dir``."""
    base = Path(output_dir)
    return {
        "staging": base / STAGING_FILENAME,
        "warehouse": base / WAREHOUSE_FILENAME,
        "quarantine": base / QUARANTINE_FILENAME,
    }


def stage_extract(source_path: str | Path, staging_path: str | Path) -> list[dict[str, str]]:
    """Read the raw source file, validate its schema, stage a copy.

    Returns the staged rows. Raises :class:`StorageError` when the
    source is missing and :class:`SchemaError` when its shape is wrong;
    both fail the extract task (and retry) rather than loading nothing.
    """
    rows = read_csv(source_path)
    validate_schema(rows)
    write_csv_atomic(staging_path, rows, ORDER_COLUMNS)
    logger.info("extract: %d raw records staged to %s", len(rows), staging_path)
    return rows


def stage_validate(
    rows: list[dict[str, str]], rules: DataQualityRules
) -> ValidationResult:
    """Run the configured data-quality rules over staged rows."""
    result = validate_rows(rows, rules)
    if result.failures:
        logger.warning(
            "validate: %d rule failures across %d records: %s",
            len(result.failures),
            result.rows_rejected,
            result.failures_by_rule,
        )
    return result


def stage_transform(
    rows: list[dict[str, str]], result: ValidationResult
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Clean, enrich, and model the rows; return (modeled, quarantined)."""
    clean, quarantined = split_clean_and_quarantine(rows, result)
    modeled = model_rows(enrich_rows(clean))
    logger.info(
        "transform: %d clean records modeled, %d quarantined",
        len(modeled),
        len(quarantined),
    )
    return modeled, quarantined


def stage_load(
    modeled: list[dict[str, str]],
    quarantined: list[dict[str, str]],
    warehouse_path: str | Path,
    quarantine_path: str | Path,
) -> int:
    """Write the warehouse partition and the quarantine file.

    The warehouse write overwrites the day's partition (idempotent).
    The quarantine file is written only when there are bad rows; when
    there are none, a stale quarantine file from an earlier run is
    removed so it cannot be mistaken for this run's rejects.
    """
    write_csv_atomic(warehouse_path, modeled, WAREHOUSE_COLUMNS)
    quarantine_path = Path(quarantine_path)
    if quarantined:
        write_csv_atomic(quarantine_path, quarantined, ORDER_COLUMNS)
    elif quarantine_path.exists():
        quarantine_path.unlink()
        logger.info("no quarantined rows; removed stale %s", quarantine_path)
    logger.info("load: %d records written to %s", len(modeled), warehouse_path)
    return len(modeled)


def run_daily_pipeline(
    source_path: str | Path,
    output_dir: str | Path,
    run_date: str,
    config: PipelineConfig | None = None,
) -> PipelineRunResult:
    """Run the full daily pipeline for ``run_date``; return metrics.

    Validation failures do not abort the run: bad rows are quarantined
    and only clean rows load. A failure here means a structural problem
    (missing file, bad schema, unparseable transform), which raises
    :class:`PipelineError` and, in Airflow, fails the task for retry.
    """
    cfg = config or load_config()
    try:
        datetime.strptime(run_date, DATE_FORMAT)
    except (TypeError, ValueError) as exc:
        raise PipelineError(f"run_date must be YYYY-MM-DD, got {run_date!r}") from exc
    paths = partition_paths(output_dir, run_date)
    rows = stage_extract(source_path, paths["staging"])
    validation = stage_validate(rows, cfg.data_quality)
    modeled, quarantined = stage_transform(rows, validation)
    loaded = stage_load(
        modeled, quarantined, paths["warehouse"], paths["quarantine"]
    )
    result = PipelineRunResult(
        run_date=run_date,
        rows_in=validation.rows_in,
        rows_clean=validation.rows_clean,
        rows_rejected=validation.rows_rejected,
        rows_loaded=loaded,
        failures_by_rule=validation.failures_by_rule,
        warehouse_path=paths["warehouse"],
        quarantine_path=paths["quarantine"] if quarantined else None,
    )
    result.log_summary()
    return result
