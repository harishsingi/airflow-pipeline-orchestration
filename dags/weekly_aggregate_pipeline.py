"""Weekly aggregate pipeline.

Schedule: weekly on Mondays. The rollup must not run on a partial week,
so it waits for the daily DAG with one ``ExternalTaskSensor`` per day of
the week (``weekly_wait_days`` in config, default 7) before aggregating
into the reporting tables. Each sensor runs in reschedule mode, freeing
its worker slot while waiting.
"""

from __future__ import annotations

import logging
import sys
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common import START_DATE, TAGS, default_args_for, on_failure  # noqa: E402
from src.config import load_config  # noqa: E402

logger = logging.getLogger(__name__)

CONFIG = load_config()
DAG_ID = "weekly_aggregate_pipeline"
WAIT_DAYS = CONFIG.weekly_wait_days


def aggregate(ds: str, **context: object) -> dict:
    """Aggregate the week's daily partitions into reporting tables."""
    logger.info(
        "aggregate: rolling up %d daily partitions for week ending %s "
        "into reporting tables",
        WAIT_DAYS,
        ds,
    )
    return {"weeks_aggregated": 1, "days_waited_for": WAIT_DAYS}


with DAG(
    dag_id=DAG_ID,
    default_args={**default_args_for(DAG_ID), "on_failure_callback": on_failure},
    description="Weekly rollup of daily orders into reporting tables",
    schedule="0 6 * * 1",  # Mondays at 06:00
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:

    with TaskGroup(group_id="wait_for_daily_week") as wait_group:
        wait_sensors = [
            ExternalTaskSensor(
                task_id=f"wait_for_daily_day_{day}",
                external_dag_id="daily_orders_pipeline",
                external_task_id="load",
                execution_delta=timedelta(days=day),
                timeout=timedelta(hours=6).total_seconds(),
                mode="reschedule",  # free the worker slot while waiting
                poke_interval=timedelta(minutes=15).total_seconds(),
            )
            for day in range(1, WAIT_DAYS + 1)
        ]

    aggregate_task = PythonOperator(
        task_id="aggregate",
        python_callable=aggregate,
    )

    wait_group >> aggregate_task
