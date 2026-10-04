"""Spark session factory with Delta wired in."""
from __future__ import annotations

from pyspark.sql import SparkSession

from lakehouse.config import get_settings


def build_session(app_name: str, shuffle_partitions: int = 200) -> SparkSession:
    cfg = get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Delta's own file compaction. Without it a 30s trigger produces about
        # 2,900 files a day per partition and reads fall off a cliff in a week.
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
    )
    if cfg.lake_root.startswith("s3a://"):
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
    return builder.getOrCreate()
