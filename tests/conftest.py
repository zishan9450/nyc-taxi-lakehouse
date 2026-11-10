import os
import time

import pytest
from pyspark.sql import SparkSession

from lakehouse.spark import ensure_java

# Naive datetimes are converted with the process timezone. CI is UTC; a laptop
# in another zone would shift pickup_hour (08:00 IST becomes 02:30 UTC).
os.environ["TZ"] = "UTC"
time.tzset()
ensure_java()


@pytest.fixture(scope="session")
def spark_plain():
    """Plain Spark (no Delta jars). For pure transform tests."""
    session = (
        SparkSession.builder.master("local[2]").appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def spark_delta():
    """Delta-enabled Spark. Must run in its own process (`pytest -m delta`): Spark's
    getOrCreate() would otherwise hand back an already-created plain session."""
    from lakehouse.spark import get_spark

    session = get_spark("delta-tests", shuffle_partitions=2)
    yield session
    session.stop()
