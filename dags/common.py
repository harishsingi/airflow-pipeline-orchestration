"""Shared defaults for every DAG in this project.

One place for the retry policy, SLA, and alerting behaviour so a new DAG
picks up the same guarantees instead of re-declaring them. The values
come from ``config/pipelines.yaml`` via ``src/config.py``; if the config
cannot be loaded (e.g. the DAGs folder was deployed without the rest of
the repo), the module falls back to the documented defaults so the DAG
still parses instead of failing the whole scheduler.

Airflow is not imported here, so this module is importable and testable
without a scheduler.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)

DEFAULT_OWNER = "data-engineering"

# Fallback policy, used only when config/pipelines.yaml cannot be read.
# It mirrors the YAML values; tests assert the two agree.
_FALLBACK_ARGS = {
    "owner": DEFAULT_OWNER,
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    # The whole daily run is expected inside 30 minutes; an SLA miss
    # alerts on-call before users notice a stale dashboard.
    "sla": timedelta(minutes=30),
}

START_DATE = datetime(2026, 11, 1, tzinfo=timezone.utc)

TAGS = ["data-engineering", "etl"]

try:
    from src.config import load_config as _load_config

    _CONFIG = _load_config()
    _START_DATE = datetime.fromisoformat(_CONFIG.start_date).replace(
        tzinfo=timezone.utc
    )
    START_DATE = _START_DATE
except Exception as exc:  # config missing/malformed: fall back, log loudly
    logger.warning("could not load config/pipelines.yaml (%s); using fallback defaults", exc)
    _CONFIG = None

DEFAULT_ARGS = dict(_FALLBACK_ARGS)


def default_args_for(dag_id: str) -> dict:
    """Per-DAG default args, from config when available.

    Retries, retry delay, and SLA come from the DAG's config entry;
    the shared guarantees (no depends_on_past, exponential backoff,
    failure email) are the same for every DAG.
    """
    args = dict(_FALLBACK_ARGS)
    if _CONFIG is not None:
        try:
            settings = _CONFIG.dag(dag_id)
        except Exception:
            return args
        args.update(
            {
                "owner": settings.owner,
                "retries": settings.retries,
                "retry_delay": timedelta(minutes=settings.retry_delay_minutes),
                "sla": timedelta(minutes=settings.sla_minutes),
            }
        )
    return args


def on_failure(context: dict) -> None:
    """Shared failure callback: log the failure so on-call can find it.

    Airflow's ``email_on_failure`` handles paging; this callback makes
    sure the dag, task, run date, and try number land in the logs even
    when email is not configured in a dev environment.
    """
    task = context.get("task_instance") or context.get("ti")
    task_id = getattr(task, "task_id", "unknown task")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown dag"
    run_id = context.get("run_id", "unknown run")
    try_number = getattr(task, "try_number", "?")
    exception = context.get("exception")
    logger.error(
        "ALERT: task %s failed in DAG %s (run %s, try %s): %s",
        task_id,
        dag_id,
        run_id,
        try_number,
        exception,
    )
