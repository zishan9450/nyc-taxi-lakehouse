"""Stage orchestration shared by the CLI and the Airflow DAG."""
from __future__ import annotations

import logging
import uuid

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from lakehouse import bronze, gold, silver
from lakehouse.config import Config
from lakehouse.ingest import fetch_month, fetch_zone_lookup, raw_path, validate_month
from lakehouse.runlog import logged_stage
from lakehouse.spark import get_spark

log = logging.getLogger(__name__)

STAGES = ("fetch", "bronze", "silver", "gold")


def stage_bronze(spark: SparkSession, cfg: Config, month: str, run_id: str, force: bool = False):
    with logged_stage(spark, cfg, run_id, month, "bronze") as info:
        res = bronze.load_month(spark, cfg, month, raw_path(cfg, month), force=force)
        info.update(rows_in=res["rows"], rows_out=res["rows"], rows_rejected=0)
    return res


def stage_silver(spark: SparkSession, cfg: Config, month: str, run_id: str):
    with logged_stage(spark, cfg, run_id, month, "silver") as info:
        res = silver.build_month(spark, cfg, month)
        info.update(
            rows_in=res["rows_in"], rows_out=res["rows_out"], rows_rejected=res["rows_rejected"]
        )
    return res


def stage_gold(spark: SparkSession, cfg: Config, month: str, run_id: str):
    with logged_stage(spark, cfg, run_id, month, "gold") as info:
        if not DeltaTable.isDeltaTable(spark, cfg.table("gold/dim_zone")):
            gold.load_zones(spark, cfg, fetch_zone_lookup(cfg))
        counts = gold.build_month(spark, cfg, month)
        info.update(rows_out=sum(counts.values()))
    return counts


def run_stage(stage: str, month: str, force: bool = False, run_id: str | None = None):
    """Run one stage in its own process. Used by Airflow so each task retries independently."""
    validate_month(month)
    if stage not in STAGES:
        raise ValueError(f"unknown stage {stage!r}; choose from {STAGES}")
    cfg = Config.from_env()
    if stage == "fetch":
        return fetch_month(cfg, month, force=force)
    spark = get_spark(f"lakehouse-{stage}-{month}")
    run_id = run_id or uuid.uuid4().hex[:12]
    if stage == "bronze":
        return stage_bronze(spark, cfg, month, run_id, force)
    if stage == "silver":
        return stage_silver(spark, cfg, month, run_id)
    return stage_gold(spark, cfg, month, run_id)


def run_month(spark: SparkSession, cfg: Config, month: str, force: bool = False) -> str:
    validate_month(month)
    run_id = uuid.uuid4().hex[:12]
    log.info("run %s: month %s", run_id, month)
    fetch_month(cfg, month, force=force)
    stage_bronze(spark, cfg, month, run_id, force)
    stage_silver(spark, cfg, month, run_id)
    stage_gold(spark, cfg, month, run_id)
    return run_id
