"""Bronze to silver: deduplicate, validate, conform.

The deduplication is the load-bearing part. At-least-once delivery means the
same transaction_id arrives more than once whenever a consumer rebalances, and
a duplicated payment in a daily total is the kind of error that is noticed by
someone outside engineering.
"""
from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.config import get_settings
from lakehouse.spark import build_session

log = logging.getLogger(__name__)

# TODO: pull this from the reference-data table instead of hard-coding it.
# Adding NOK meant a redeploy of both streams, which is silly.
VALID_CURRENCIES = ("EUR", "USD", "GBP", "CHF", "SEK")


def conform(df: DataFrame, watermark: str) -> DataFrame:
    return (
        df.withWatermark("occurred_at", watermark)
        # Within the watermark, keep the first arrival per id. Spark holds the
        # id set in state only for the watermark span, so this is bounded.
        .dropDuplicates(["transaction_id"])
        .filter(F.col("amount").isNotNull() & (F.col("amount") != 0))
        .filter(F.col("currency").isin(*VALID_CURRENCIES))
        .withColumn("amount", F.round("amount", 2))
        .withColumn("country", F.upper(F.coalesce("country", F.lit("XX"))))
        .withColumn("channel", F.lower(F.coalesce("channel", F.lit("unknown"))))
        .withColumn("is_high_value", F.col("amount") > 10_000)
        .withColumn("event_date", F.to_date("occurred_at"))
        .drop("_raw")
    )


def _upsert(batch: DataFrame, _: int) -> None:
    cfg = get_settings()
    spark = batch.sparkSession
    if not DeltaTable.isDeltaTable(spark, cfg.silver):
        batch.write.format("delta").partitionBy("event_date").save(cfg.silver)
        return
    (
        DeltaTable.forPath(spark, cfg.silver)
        .alias("t")
        .merge(batch.alias("s"), "t.transaction_id = s.transaction_id")
        # Only the late-arriving correction should win. Without the condition
        # a replay would overwrite good rows with older ones.
        .whenMatchedUpdateAll(condition="s.occurred_at > t.occurred_at")
        .whenNotMatchedInsertAll()
        .execute()
    )


def run(await_termination: bool = True):
    cfg = get_settings()
    spark = build_session("silver-conform")
    stream = spark.readStream.format("delta").load(cfg.bronze)

    query = (
        conform(stream, cfg.late_arrival_watermark)
        .writeStream.foreachBatch(_upsert)
        .option("checkpointLocation", f"{cfg.checkpoint_root}/silver")
        .trigger(processingTime=cfg.trigger_interval)
        .start()
    )
    log.info("silver stream started id=%s", query.id)
    if await_termination:
        query.awaitTermination()
    return query


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
