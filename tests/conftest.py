import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Local Delta-enabled session. Two shuffle partitions because the test
    data is tiny and 200 partitions of nothing costs seconds per assertion."""
    session = (
        SparkSession.builder.appName("tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
