"""Airflow DAG: one run per month, four tasks, each retried independently.

`{{ ds }}` is the first day of the data interval, so the run that executes on Feb 1 processes
January. catchup is off because TLC publishes with a lag; backfill explicitly with
`airflow dags backfill` or `python -m lakehouse backfill`.
"""
import itertools
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lakehouse.pipeline import STAGES, run_stage

with DAG(
    dag_id="nyc_lakehouse_monthly",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["lakehouse", "delta", "pyspark"],
) as dag:
    tasks = [
        PythonOperator(
            task_id=stage,
            python_callable=run_stage,
            op_kwargs={"stage": stage, "month": "{{ ds[:7] }}", "run_id": "{{ run_id }}"},
            # fetch returns a filesystem path, which Airflow cannot store as the task result.
            do_xcom_push=False,
        )
        for stage in STAGES
    ]
    for upstream, downstream in itertools.pairwise(tasks):
        upstream >> downstream
