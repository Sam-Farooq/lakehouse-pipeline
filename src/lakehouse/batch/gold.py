"""Silver to gold, then out to BigQuery.

Runs daily from Airflow rather than as a stream. The aggregates are read by
dashboards that refresh each morning, and a continuously-updating gold table
would cost cluster time all night to serve nobody.
"""
from __future__ import annotations

import argparse
import logging
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.config import get_settings
from lakehouse.spark import build_session

log = logging.getLogger(__name__)


def daily_account_activity(silver: DataFrame, run_date: date) -> DataFrame:
    return (
        silver.filter(F.col("event_date") == F.lit(run_date))
        .groupBy("event_date", "account_id", "currency")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.round(F.max("amount"), 2).alias("max_amount"),
            F.sum(F.col("is_high_value").cast("int")).alias("high_value_count"),
            F.countDistinct("counterparty_id").alias("distinct_counterparties"),
            F.countDistinct("country").alias("distinct_countries"),
        )
    )


def corridor_volume(silver: DataFrame, run_date: date) -> DataFrame:
    return (
        silver.filter(F.col("event_date") == F.lit(run_date))
        .groupBy("event_date", "country", "channel")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            # p95 rather than max: one outlier should not define the corridor.
            F.round(F.expr("percentile_approx(amount, 0.95)"), 2).alias("p95_amount"),
        )
    )


def run(run_date: date) -> None:
    cfg = get_settings()
    spark = build_session("gold-aggregate", shuffle_partitions=64)
    silver = spark.read.format("delta").load(cfg.silver)

    for name, df in (
        ("daily_account_activity", daily_account_activity(silver, run_date)),
        ("corridor_volume", corridor_volume(silver, run_date)),
    ):
        path = f"{cfg.gold}/{name}"
        (
            df.write.format("delta")
            .mode("overwrite")
            # Rewrite only this run's partition. A plain overwrite would drop
            # history every morning, which is how a backfill loses a year.
            .option("replaceWhere", f"event_date = '{run_date}'")
            .partitionBy("event_date")
            .save(path)
        )
        log.info("wrote %s rows=%d", name, df.count())

        if cfg.bq_project:
            (
                df.write.format("bigquery")
                .option("table", f"{cfg.bq_project}.{cfg.bq_dataset}.{name}")
                .option("partitionField", "event_date")
                .mode("append")
                .save()
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, type=date.fromisoformat)
    run(ap.parse_args().date)
