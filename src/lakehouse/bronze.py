"""Bronze: raw data as delivered, plus ingest metadata. Reloading a month replaces that month."""
from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import manifest
from lakehouse.config import Config
from lakehouse.deltaio import replace_partition
from lakehouse.ingest import sha256_file, validate_month
from lakehouse.transforms import lowercase_columns

log = logging.getLogger(__name__)


def load_month(
    spark: SparkSession, cfg: Config, month: str, source_path: Path | str, force: bool = False
) -> dict:
    """Load one source file into bronze. Returns {"skipped": bool, "rows": int}.

    Skips when the manifest already holds this month with the same file checksum. If the
    checksum differs (upstream republished the file) the month is replaced.
    """
    validate_month(month)
    source_path = Path(source_path)
    digest = sha256_file(source_path)

    existing = manifest.get(spark, cfg, month)
    if (
        not force
        and existing is not None
        and existing["status"] == "loaded"
        and existing["sha256"] == digest
    ):
        log.info("bronze %s already loaded (same checksum), skipping", month)
        return {"skipped": True, "rows": int(existing["row_count"])}

    df = (
        lowercase_columns(spark.read.parquet(str(source_path)))
        .withColumn("_ingest_month", F.lit(month))
        .withColumn("_source_file", F.lit(source_path.name))
        .withColumn("_sha256", F.lit(digest))
        .withColumn("_ingested_at", F.current_timestamp())
        .cache()
    )
    try:
        rows = df.count()
        replace_partition(spark, df, cfg.table("bronze/trips"), "_ingest_month", month)
    finally:
        df.unpersist()

    manifest.upsert(spark, cfg, month, digest, source_path.name, rows)
    log.info("bronze %s loaded: %d rows", month, rows)
    return {"skipped": False, "rows": rows}
