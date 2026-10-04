"""Daily gold build.

The streams run continuously on Kubernetes and are not Airflow's business.
This DAG owns the part that has a date: quality gates, aggregation, dbt, and
the freshness check that decides whether any of it should run at all.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator

TZ = pendulum.timezone("Europe/Berlin")


@dag(
    dag_id="lakehouse_daily",
    schedule="30 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=10)},
    tags=["lakehouse", "gold"],
)
def lakehouse_daily():

    @task
    def check_freshness(data_interval_start=None) -> str:
        """Refuse to aggregate a day the stream never finished writing.

        Running anyway produces a gold partition that looks complete, gets
        cached by the dashboard, and is quietly wrong until someone notices
        the totals are low.
        """
        from pyspark.sql import functions as F

        from lakehouse.config import get_settings
        from lakehouse.spark import build_session

        cfg = get_settings()
        spark = build_session("freshness-check", shuffle_partitions=8)
        day = data_interval_start.date()

        latest = (
            spark.read.format("delta").load(cfg.silver)
            .filter(F.col("event_date") == F.lit(day))
            .agg(F.max("_ingested_at").alias("m"))
            .collect()[0]["m"]
        )
        if latest is None:
            raise AirflowSkipException(f"no silver rows for {day}")
        return day.isoformat()

    @task
    def quality_gate(run_date: str) -> dict:
        from pyspark.sql import functions as F

        from lakehouse.config import get_settings
        from lakehouse.quality.expectations import assert_blocking, run_checks
        from lakehouse.spark import build_session

        cfg = get_settings()
        spark = build_session("quality-gate", shuffle_partitions=32)
        df = (
            spark.read.format("delta").load(cfg.silver)
            .filter(F.col("event_date") == F.lit(run_date))
        )
        results = run_checks(df)
        assert_blocking(results)
        return {r.name: r.violations for r in results}

    @task
    def aggregate(run_date: str) -> str:
        from datetime import date

        from lakehouse.batch.gold import run

        run(date.fromisoformat(run_date))
        return run_date

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --target prod --select marts",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --target prod --select marts",
    )

    run_date = check_freshness()
    quality_gate(run_date) >> aggregate(run_date) >> dbt_run >> dbt_test


lakehouse_daily()
