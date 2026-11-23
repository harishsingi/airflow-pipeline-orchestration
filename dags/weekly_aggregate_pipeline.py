"""Weekly aggregate pipeline.

Schedule: weekly on Mondays. The rollup must not run on a partial week, so
it waits for the daily DAG with an ExternalTaskSensor for each day of the
week before aggregating into the reporting tables.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from common import DEFAULT_ARGS, START_DATE, TAGS


def aggregate(**context):
    """Aggregate the week's daily partitions into reporting tables."""
    ds = context["ds"]
    print(f"aggregate: rolling up week ending {ds} into reporting tables")
    return {"weeks_aggregated": 1}


with DAG(
    dag_id="weekly_aggregate_pipeline",
    default_args=DEFAULT_ARGS,
    description="Weekly rollup of daily orders into reporting tables",
    schedule_interval="0 6 * * 1",  # Mondays at 06:00
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:

    wait_for_daily = ExternalTaskSensor(
        task_id="wait_for_daily_orders",
        external_dag_id="daily_orders_pipeline",
        external_task_id="load",
        execution_delta=timedelta(days=1),
        timeout=timedelta(hours=6).total_seconds(),
        mode="reschedule",  # free the worker slot while waiting
        poke_interval=timedelta(minutes=15).total_seconds(),
    )

    aggregate_task = PythonOperator(
        task_id="aggregate",
        python_callable=aggregate,
    )

    wait_for_daily >> aggregate_task
