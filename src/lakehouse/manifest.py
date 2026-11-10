"""Manifest: which source file (by checksum) was loaded for each month."""
from __future__ import annotations

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from lakehouse.config import Config

SCHEMA = StructType([
    StructField("month", StringType()),
    StructField("sha256", StringType()),
    StructField("source_file", StringType()),
    StructField("row_count", LongType()),
    StructField("status", StringType()),
    StructField("loaded_at", TimestampType()),
])


def get(spark: SparkSession, cfg: Config, month: str) -> Row | None:
    path = cfg.table("meta/manifest")
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    rows = spark.read.format("delta").load(path).filter(F.col("month") == month).limit(1).collect()
    return rows[0] if rows else None


def upsert(
    spark: SparkSession, cfg: Config, month: str, sha256: str, source_file: str,
    row_count: int, status: str = "loaded",
) -> None:
    path = cfg.table("meta/manifest")
    new = spark.createDataFrame(
        [(month, sha256, source_file, row_count, status, datetime.now(timezone.utc))], SCHEMA
    )
    if DeltaTable.isDeltaTable(spark, path):
        (
            DeltaTable.forPath(spark, path).alias("t")
            .merge(new.alias("s"), "t.month = s.month")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        new.write.format("delta").save(path)
