"""Silver: typed, validated, de-duplicated trips. Rejected rows go to quarantine with a reason."""
from __future__ import annotations

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import Config
from lakehouse.deltaio import replace_partition
from lakehouse.ingest import validate_month
from lakehouse.quality import check_not_empty, check_reject_rate, check_unique_key
from lakehouse.transforms import flag, split_flagged

log = logging.getLogger(__name__)


def upsert_silver(spark: SparkSession, good: DataFrame, path: str, month: str) -> None:
    """MERGE one month into silver.

    NOT MATCHED BY SOURCE DELETE (scoped to the month) makes silver mirror the latest clean
    data exactly, so a corrected upstream file also removes rows that no longer exist.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        good.write.format("delta").partitionBy("pickup_month").save(path)
        return
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(good.alias("s"), f"t.pickup_month = '{month}' AND t.trip_id = s.trip_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(condition=f"t.pickup_month = '{month}'")
        .execute()
    )


def build_month(spark: SparkSession, cfg: Config, month: str) -> dict:
    validate_month(month)
    bronze = (
        spark.read.format("delta").load(cfg.table("bronze/trips"))
        .filter(F.col("_ingest_month") == month)
    )
    flagged = flag(bronze, month).persist()
    good = None
    bad = None
    try:
        total = flagged.count()
        check_not_empty(total, f"bronze month {month}")

        good, bad = split_flagged(flagged)
        good = good.persist()
        bad = bad.persist()
        rejected = bad.count()
        written = good.count()
        duplicates = total - rejected - written
        # The parent cache is redundant once both sides are materialized, and the MERGE
        # below also reads Parquet. Holding all three copies is what exhausts the heap.
        flagged.unpersist()
        flagged = None

        # Quarantine first, so rejected rows can be inspected even when the gate below fails.
        replace_partition(
            spark,
            bad.withColumn("_ingest_month", F.lit(month)),
            cfg.table("silver/trips_quarantine"),
            "_ingest_month",
            month,
        )
        bad.unpersist()

        rate = check_reject_rate(rejected, total, cfg.max_reject_rate)
        check_unique_key(good)
        upsert_silver(spark, good, cfg.table("silver/trips"), month)
    finally:
        # A heap failure stops the context; unpersist must not hide that error.
        for frame in (flagged, good, bad):
            if frame is None:
                continue
            try:
                frame.unpersist()
            except Exception:
                log.debug("unpersist skipped", exc_info=True)

    log.info(
        "silver %s: in=%d out=%d rejected=%d (%.2f%%) duplicates=%d",
        month, total, written, rejected, rate * 100, duplicates,
    )
    return {
        "rows_in": total, "rows_out": written, "rows_rejected": rejected,
        "duplicates_removed": duplicates, "reject_rate": rate,
    }
