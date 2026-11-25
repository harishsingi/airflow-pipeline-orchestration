"""CSV storage helpers: read, and atomic write.

Writes are atomic (write to a temporary file in the same directory,
then ``os.replace``) so a crashed or retried load can never leave a
half-written warehouse file behind, and re-running a load for the same
partition overwrites it with identical content: the load is idempotent.
"""

from __future__ import annotations

import csv
import logging
import os
import tempfile
from pathlib import Path
from typing import Sequence

from src.exceptions import StorageError

logger = logging.getLogger(__name__)


def read_csv(path: str | Path) -> list[dict[str, str]]:
    """Read a CSV file into a list of row dicts.

    Raises :class:`StorageError` when the file is missing or unreadable,
    so a missing extract fails the task (and retries) instead of
    silently producing an empty load.
    """
    path = Path(path)
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    except FileNotFoundError as exc:
        raise StorageError(f"input file not found: {path}") from exc
    except OSError as exc:
        raise StorageError(f"could not read {path}: {exc}") from exc
    if rows and any(value is None for row in rows for value in row.values()):
        raise StorageError(
            f"{path} has ragged rows (a row with more/fewer fields than the header)"
        )
    logger.info("read %d rows from %s", len(rows), path)
    return rows


def write_csv_atomic(
    path: str | Path,
    rows: Sequence[dict[str, str]],
    fieldnames: Sequence[str],
) -> int:
    """Write ``rows`` to ``path`` atomically; return rows written.

    The parent directory is created if needed. An empty ``rows`` list
    still writes the header, so a downstream reader never sees a
    missing file just because a partition had zero clean rows.
    """
    path = Path(path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
                writer.writeheader()
                writer.writerows(rows)
            os.replace(tmp_name, path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
    except OSError as exc:
        raise StorageError(f"could not write {path}: {exc}") from exc
    logger.info("wrote %d rows to %s", len(rows), path)
    return len(rows)
