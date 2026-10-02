"""OPTIONAL: the same daily pipeline as an Apache Airflow 3 DAG.

GitHub Actions already runs this project every day for free. This file shows how the
exact same steps map onto Airflow, the scheduler most data teams use:

    ingest  ->  dbt_build  ->  dbt_source_freshness  ->  publish_report

Each box is a separate task, so Airflow can retry just the step that failed and shows
exactly where a run broke. To use it:
  1. pip install -e .  (this project) in the same environment as Airflow
  2. set AQI_REPO_ROOT, AQI_TARGET=snowflake and the SNOWFLAKE_* variables for the Airflow worker
     (in production they would come from an Airflow Connection or a secrets backend)
  3. copy this file into Airflow's dags/ folder
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# dbt runs from inside the dbt/ folder so its default relative paths (../data/...) resolve.
# AQI_DBT_TARGET picks the Snowflake ("prod") or DuckDB ("dev") output in profiles.yml.
DBT = (
    'export AQI_DBT_TARGET=$([ "$AQI_TARGET" = "snowflake" ] && echo prod || echo dev) && '
    'cd "$AQI_REPO_ROOT/dbt" && dbt {cmd} --profiles-dir .'
)

with DAG(
    dag_id="india_air_quality_daily",
    description="Open-Meteo -> Parquet -> Snowflake (PUT/COPY/MERGE) -> dbt -> tested AQI marts -> dashboard",
    schedule="30 1 * * *",  # 07:00 IST, same as the GitHub Actions workflow
    start_date=datetime(2026, 1, 1),
    catchup=False,  # history is loaded with `aqi backfill`, not by replaying old runs
    max_active_runs=1,  # never two loads at once
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),  # the API may be briefly down
    },
    tags=["air-quality", "snowflake", "dbt"],
) as dag:
    ingest = BashOperator(task_id="ingest", bash_command="aqi ingest --days 3")

    dbt_build = BashOperator(
        task_id="dbt_build",
        # keep a copy of the build results for the dashboard's health panel
        bash_command=DBT.format(cmd="build") + " && cp target/run_results.json target/build_results.json",
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=DBT.format(cmd="source freshness"),
    )

    publish_report = BashOperator(task_id="publish_report", bash_command="aqi report")

    ingest >> dbt_build >> dbt_freshness >> publish_report
