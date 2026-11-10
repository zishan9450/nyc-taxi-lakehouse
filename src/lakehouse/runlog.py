"""Run log: one row per (run, month, stage) with row counts and duration."""
from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse.config import Config

log = logging.getLogger(__name__)

SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("month", StringType()),
    StructField("stage", StringType()),
    StructField("status", StringType()),
    StructField("rows_in", LongType()),
    StructField("rows_out", LongType()),
    StructField("rows_rejected", LongType()),
    StructField("duration_s", DoubleType()),
    StructField("error", StringType()),
    StructField("logged_at", TimestampType()),
])


def _append(spark: SparkSession, cfg: Config, rec: tuple) -> None:
    (
        spark.createDataFrame([rec], SCHEMA)
        .write.format("delta").mode("append").save(cfg.table("meta/run_log"))
    )


@contextmanager
def logged_stage(
    spark: SparkSession, cfg: Config, run_id: str, month: str, stage: str
) -> Iterator[dict]:
    """Time a stage and record its outcome. Fill the yielded dict with rows_in/out/rejected."""
    info: dict = {}
    start = time.time()

    def record(status: str, error: str | None) -> None:
        try:
            _append(spark, cfg, (
                run_id, month, stage, status,
                info.get("rows_in"), info.get("rows_out"), info.get("rows_rejected"),
                round(time.time() - start, 2), error, datetime.now(timezone.utc),
            ))
        except Exception:
            log.warning("run log write failed for %s/%s", month, stage, exc_info=True)

    try:
        yield info
    except Exception as exc:
        record("failed", str(exc)[:500])
        raise
    else:
        record("success", None)
