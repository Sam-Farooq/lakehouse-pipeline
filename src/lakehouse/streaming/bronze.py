"""Kafka to bronze.

Append only, no parsing beyond the schema cast, no deduplication. Bronze is
the replay log: if silver has a bug the fix is to rebuild silver from bronze,
which is only possible if bronze never dropped anything.
"""
from __future__ import annotations

import logging

from pyspark.sql import functions as F

from lakehouse.config import get_settings
from lakehouse.schemas import TRANSACTION
from lakehouse.spark import build_session

log = logging.getLogger(__name__)


def run(await_termination: bool = True):
    cfg = get_settings()
    spark = build_session("bronze-ingest")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_brokers)
        .option("subscribe", cfg.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
        # Spark's own retry handles a broker bounce. Failing the query means
        # a Kubernetes restart and a cold JVM for a five-second outage.
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(
            F.from_json(F.col("value").cast("string"), TRANSACTION).alias("t"),
            F.col("value").cast("string").alias("_raw"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").cast("string").alias("_kafka_offset"),
        )
        .select("t.*", "_raw", "_kafka_partition", "_kafka_offset")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("event_date", F.to_date("occurred_at"))
    )

    query = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("event_date")
        .option("checkpointLocation", f"{cfg.checkpoint_root}/bronze")
        .trigger(processingTime=cfg.trigger_interval)
        .start(cfg.bronze)
    )
    log.info("bronze stream started id=%s", query.id)
    if await_termination:
        query.awaitTermination()
    return query


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
