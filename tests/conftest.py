import os
import sys

import pytest
from pyspark.sql import SparkSession

# Spark launches its Python workers with whatever "python3" resolves to on
# PATH, not the interpreter running the driver. On any machine with a system
# python older than the venv that is a PYTHON_VERSION_MISMATCH at the first
# shuffle, several layers down a Py4J stack trace.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    """Plain local session, deliberately without the Delta extensions.

    These tests exercise conform(), the quality checks and the gold aggregates,
    which are pure DataFrame transforms. Loading the Delta catalog would pull
    the jars from Maven on first run, so it would add a network dependency and
    about ninety seconds to CI in exchange for nothing.
    """
    session = (
        SparkSession.builder.appName("tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# Schemas are declared rather than inferred. Several fixtures below hold a
# column that is null in every row, and inference on those raises
# CANNOT_DETERMINE_TYPE rather than defaulting to string.

BRONZE_IN = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("counterparty_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("mcc", IntegerType(), True),
    StructField("occurred_at", TimestampType(), False),
    StructField("_raw", StringType(), True),
])

QUALITY_IN = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("occurred_at", TimestampType(), True),
])

SILVER_OUT = StructType([
    StructField("event_date", DateType(), True),
    StructField("account_id", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("counterparty_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("is_high_value", BooleanType(), True),
    StructField("occurred_at", TimestampType(), True),
])
