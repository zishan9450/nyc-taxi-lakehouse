"""Gold: small, query-ready aggregates for BI and the analytics assistant."""
from __future__ import annotations

import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.config import Config
from lakehouse.deltaio import replace_partition
from lakehouse.ingest import validate_month

log = logging.getLogger(__name__)

CARD_PAYMENT_TYPE = 1  # cash tips are not recorded in TLC data, so tip rate uses card trips only


def load_zones(spark: SparkSession, cfg: Config, csv_path: Path | str) -> None:
    """Dimension table: taxi zone id -> borough / zone name. Small, fully rewritten."""
    zones = (
        spark.read.option("header", True).csv(str(csv_path))
        .select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
            F.col("service_zone"),
        )
    )
    (
        zones.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .save(cfg.table("gold/dim_zone"))
    )


def build_month(spark: SparkSession, cfg: Config, month: str) -> dict:
    validate_month(month)
    zones_path = cfg.table("gold/dim_zone")
    if not DeltaTable.isDeltaTable(spark, zones_path):
        raise RuntimeError("gold/dim_zone is missing; call load_zones() first")

    silver = (
        spark.read.format("delta").load(cfg.table("silver/trips"))
        .filter(F.col("pickup_month") == month)
    )
    zones = spark.read.format("delta").load(zones_path)

    trips_by_zone_day = silver.groupBy("pickup_month", "pickup_date", "pu_location_id").agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("revenue"),
        F.avg("trip_distance").alias("avg_distance_miles"),
        F.avg("trip_duration_min").alias("avg_duration_min"),
    )

    revenue_by_hour = silver.groupBy("pickup_month", "pickup_date", "pickup_hour").agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("revenue"),
        F.sum("fare_amount").alias("fare_total"),
    )

    card = silver.filter(F.col("payment_type") == CARD_PAYMENT_TYPE)
    tip_rate_by_borough = (
        card.join(zones, card["pu_location_id"] == zones["location_id"], "left")
        .groupBy("pickup_month", F.coalesce(F.col("borough"), F.lit("Unknown")).alias("borough"))
        .agg(
            F.count("*").alias("card_trips"),
            F.sum("tip_amount").alias("tip_total"),
            F.sum("fare_amount").alias("fare_total"),
        )
        .withColumn(
            "tip_rate",
            F.when(F.col("fare_total") > 0, F.col("tip_total") / F.col("fare_total")),
        )
    )

    outputs = {
        "gold/trips_by_zone_day": trips_by_zone_day,
        "gold/revenue_by_hour": revenue_by_hour,
        "gold/tip_rate_by_borough": tip_rate_by_borough,
    }
    counts: dict[str, int] = {}
    for name, df in outputs.items():
        replace_partition(spark, df, cfg.table(name), "pickup_month", month)
        counts[name] = (
            spark.read.format("delta").load(cfg.table(name))
            .filter(F.col("pickup_month") == month).count()
        )
    log.info("gold %s: %s", month, counts)
    return counts
