from __future__ import annotations

import glob
import os
import shutil
import subprocess
import time

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Same rule as the tests: session timezone is UTC, so the JVM and the process
# must be UTC too. Otherwise naive TLC timestamps shift by the machine offset.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

# Homebrew on the Mac, and the OpenJDK package in the Docker images.
# /usr/bin/java on macOS is a stub, so a path is accepted only when java -version works.
_JAVA_CANDIDATES = (
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home",
    "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/usr/local/opt/openjdk/libexec/openjdk.jdk/Contents/Home",
    "/usr/lib/jvm/default-java",
)


def _java_home_usable(home: str | None) -> bool:
    if not home:
        return False
    java = os.path.join(home, "bin", "java")
    return os.path.isfile(java) and os.access(java, os.X_OK)


def _java_binary_works(java: str) -> bool:
    try:
        completed = subprocess.run(
            [java, "-version"], capture_output=True, timeout=15, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return completed.returncode == 0


def _use_java_home(home: str) -> str:
    os.environ["JAVA_HOME"] = home
    os.environ["PATH"] = os.path.join(home, "bin") + os.pathsep + os.environ.get("PATH", "")
    return home


def _candidate_homes() -> list[str]:
    homes = list(_JAVA_CANDIDATES)
    homes.extend(sorted(glob.glob("/usr/lib/jvm/java-17-openjdk-*")))
    java_bin = shutil.which("java")
    if java_bin:
        bindir = os.path.dirname(os.path.realpath(java_bin))
        if os.path.basename(bindir) == "bin":
            homes.append(os.path.dirname(bindir))
    return homes


def ensure_java() -> str:
    """Set JAVA_HOME to a real JDK. Spark inherits this when it launches the gateway."""
    current = os.environ.get("JAVA_HOME")
    if current and _java_home_usable(current):
        return current
    for home in _candidate_homes():
        java = os.path.join(home, "bin", "java")
        if _java_home_usable(home) and _java_binary_works(java):
            return _use_java_home(home)
    raise RuntimeError("No Java runtime found. Install OpenJDK 17 and set JAVA_HOME to it.")


def get_spark(app_name: str = "lakehouse", shuffle_partitions: int = 8) -> SparkSession:
    """Local Spark session with Delta Lake enabled.

    Schema auto-merge lets MERGE and appends accept new upstream columns instead of failing.
    A monthly file is about 3 million rows. local[*] on a laptop runs one task per core, and
    each task reads a whole Parquet row group into the driver heap, so the default 1g heap
    dies once several months are in silver. Two cores and a 6g heap fit a 16GB machine.
    Object hash aggregation cannot spill; sort aggregation can.
    Override with SPARK_DRIVER_MEMORY and SPARK_MASTER.
    """
    ensure_java()
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "6g")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.execution.useObjectHashAggregateExec", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
