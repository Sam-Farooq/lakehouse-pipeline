"""Explicit schemas for every layer.

Spark will infer a schema from JSON and it will be wrong the first time a field
arrives null across a whole micro-batch. Inference is also a full extra pass
over the data. Both reasons to declare it.
"""
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

# What the producer puts on the topic.
TRANSACTION = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=False),
    StructField("counterparty_id", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=False),
    StructField("currency", StringType(), nullable=False),
    StructField("country", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=True),
    StructField("mcc", IntegerType(), nullable=True),
    StructField("occurred_at", TimestampType(), nullable=False),
])

# Bronze keeps the raw payload alongside the parsed columns. When a downstream
# bug turns out to be an upstream contract change, the original bytes are the
# only thing that settles it.
BRONZE_EXTRA = [
    StructField("_raw", StringType(), nullable=True),
    StructField("_kafka_partition", IntegerType(), nullable=True),
    StructField("_kafka_offset", StringType(), nullable=True),
    StructField("_ingested_at", TimestampType(), nullable=False),
]

BRONZE = StructType(TRANSACTION.fields + BRONZE_EXTRA)
