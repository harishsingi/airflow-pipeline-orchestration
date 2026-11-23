"""Daily orders ETL pipeline.

Schedule: daily, catchup off (a missed day backfills on demand, never by
surprise). Chain: extract -> validate -> transform (TaskGroup) -> load ->
notify. Validation is a task in the DAG, so a rule failure blocks the load
and triggers Airflow's retry instead of letting bad data through.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from common import DEFAULT_ARGS, START_DATE, TAGS
from operators.data_quality_operator import DataQualityOperator


def extract(**context):
    """Pull the day's raw order records into staging."""
    ds = context["ds"]
    print(f"extract: staging raw orders for {ds}")
    return {"staged_records": 120, "partition": ds}


def clean(**context):
    """Drop malformed rows and normalise types."""
    print("transform.clean: dropping malformed rows")
    return {"clean_records": 118}


def enrich(**context):
    """Join reference data and derive reporting fields."""
    print("transform.enrich: deriving reporting fields")
    return {"enriched_records": 118}


def model(**context):
    """Shape records into the warehouse model."""
    print("transform.model: shaping warehouse model")
    return {"modeled_records": 118}


def load(**context):
    """Write the clean records to the warehouse, replacing the partition."""
    ds = context["ds"]
    print(f"load: writing 118 records to warehouse/orders ({ds})")
    return {"loaded_records": 118, "partition": ds}


def notify(**context):
    """Log the outcome; failures reach this via on_failure_callback."""
    ti = context["ti"]
    loaded = ti.xcom_pull(task_ids="load")
    print(f"notify: daily_orders_pipeline succeeded, {loaded['loaded_records']} rows")


def on_failure(context):
    ti = context["ti"]
    print(f"ALERT: task {ti.task_id} failed in daily_orders_pipeline, paging on-call")


with DAG(
    dag_id="daily_orders_pipeline",
    default_args={**DEFAULT_ARGS, "on_failure_callback": on_failure},
    description="Daily orders ETL: extract, validate, transform, load",
    schedule_interval=timedelta(days=1),
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    validate_task = DataQualityOperator(
        task_id="validate",
        dataset="staging/orders",
        rules={
            "not_null": ["order_id", "customer_id", "amount"],
            "unique": ["order_id"],
            "range": {"amount": [0, 10000]},
        },
    )

    with TaskGroup(group_id="transform") as transform_group:
        clean_task = PythonOperator(task_id="clean", python_callable=clean)
        enrich_task = PythonOperator(task_id="enrich", python_callable=enrich)
        model_task = PythonOperator(task_id="model", python_callable=model)
        clean_task >> enrich_task >> model_task

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    notify_task = PythonOperator(
        task_id="notify",
        python_callable=notify,
        trigger_rule="all_success",
    )

    extract_task >> validate_task >> transform_group >> load_task >> notify_task
