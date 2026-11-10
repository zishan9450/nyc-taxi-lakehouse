"""End-to-end behaviour on tiny files: idempotency, schema evolution, MERGE, gates.

Run in its own process: `pytest -m delta` (needs the Delta jars from Maven).
"""
from dataclasses import replace
from datetime import datetime

import pandas as pd
import pytest
from delta.tables import DeltaTable
from helpers import make_rows
from pyspark.sql import functions as F

from lakehouse import bronze, gold, silver
from lakehouse.config import Config
from lakehouse.quality import QualityError

pytestmark = pytest.mark.delta


@pytest.fixture()
def cfg(tmp_path):
    return Config(lake_root=tmp_path / "lake", raw_dir=tmp_path / "raw", max_reject_rate=0.5)


def at(month: str, day: int, hour: int) -> dict:
    return {
        "tpep_pickup_datetime": datetime(2024, int(month[5:]), day, hour, 0),
        "tpep_dropoff_datetime": datetime(2024, int(month[5:]), day, hour, 20),
    }


def write_month(tmp_path, name: str, rows: list[dict], rename: dict | None = None):
    df = pd.DataFrame(rows)
    if rename:
        df = df.rename(columns=rename)
    path = tmp_path / f"{name}.parquet"
    df.to_parquet(path, index=False, coerce_timestamps="us")
    return path


def jan_rows(extra_bad: bool = True):
    rows = [at("2024-01", 15, 8), {**at("2024-01", 15, 9), "fare_amount": 20.0, "total_amount": 28.0}]
    if extra_bad:
        rows.append({**at("2024-01", 15, 10), "fare_amount": -5.0})
    return make_rows(*rows)


def bronze_df(spark, cfg):
    return spark.read.format("delta").load(cfg.table("bronze/trips"))


def test_bronze_reload_is_idempotent(spark_delta, cfg, tmp_path):
    f = write_month(tmp_path, "jan", jan_rows())
    first = bronze.load_month(spark_delta, cfg, "2024-01", f)
    assert first == {"skipped": False, "rows": 3}
    assert bronze.load_month(spark_delta, cfg, "2024-01", f)["skipped"] is True
    assert bronze.load_month(spark_delta, cfg, "2024-01", f, force=True)["skipped"] is False
    assert bronze_df(spark_delta, cfg).count() == 3


def test_changed_source_file_replaces_the_month(spark_delta, cfg, tmp_path):
    f = write_month(tmp_path, "jan", jan_rows())
    bronze.load_month(spark_delta, cfg, "2024-01", f)
    f = write_month(tmp_path, "jan", jan_rows(extra_bad=False))  # upstream republished
    res = bronze.load_month(spark_delta, cfg, "2024-01", f)
    assert res["skipped"] is False
    assert bronze_df(spark_delta, cfg).count() == 2


def test_schema_evolution_and_column_case(spark_delta, cfg, tmp_path):
    jan = write_month(tmp_path, "jan", jan_rows(), rename={"airport_fee": "Airport_fee"})
    feb_rows = [{**r, **at("2024-02", 10, 8), "cbd_congestion_fee": 0.75} for r in make_rows({}, {})]
    feb_rows[1].update(at("2024-02", 10, 9))
    feb = write_month(tmp_path, "feb", feb_rows)

    bronze.load_month(spark_delta, cfg, "2024-01", jan)
    bronze.load_month(spark_delta, cfg, "2024-02", feb)

    df = bronze_df(spark_delta, cfg)
    assert "airport_fee" in df.columns and "Airport_fee" not in df.columns
    assert "cbd_congestion_fee" in df.columns
    assert df.filter((F.col("_ingest_month") == "2024-01") & F.col("cbd_congestion_fee").isNull()).count() == 3
    assert df.filter((F.col("_ingest_month") == "2024-02") & F.col("cbd_congestion_fee").isNotNull()).count() == 2


def test_silver_merge_is_idempotent_and_quarantines_bad_rows(spark_delta, cfg, tmp_path):
    bronze.load_month(spark_delta, cfg, "2024-01", write_month(tmp_path, "jan", jan_rows()))
    res = silver.build_month(spark_delta, cfg, "2024-01")
    assert (res["rows_in"], res["rows_out"], res["rows_rejected"]) == (3, 2, 1)

    silver.build_month(spark_delta, cfg, "2024-01")  # re-run: no duplicates
    assert spark_delta.read.format("delta").load(cfg.table("silver/trips")).count() == 2

    quarantine = spark_delta.read.format("delta").load(cfg.table("silver/trips_quarantine"))
    assert [r["reject_reason"] for r in quarantine.collect()] == ["negative_fare"]


def test_gate_blocks_silver_when_reject_rate_is_too_high(spark_delta, cfg, tmp_path):
    rows = make_rows(
        {**at("2024-01", 15, 8), "fare_amount": -1.0},
        {**at("2024-01", 15, 9), "fare_amount": -2.0},
    )
    bronze.load_month(spark_delta, cfg, "2024-01", write_month(tmp_path, "jan", rows))
    strict = replace(cfg, max_reject_rate=0.05)
    with pytest.raises(QualityError):
        silver.build_month(spark_delta, strict, "2024-01")
    assert not DeltaTable.isDeltaTable(spark_delta, strict.table("silver/trips"))
    assert DeltaTable.isDeltaTable(spark_delta, strict.table("silver/trips_quarantine"))


def test_gold_tables_reconcile_with_silver(spark_delta, cfg, tmp_path):
    zones = spark_delta.createDataFrame(
        [(161, "Manhattan", "Midtown Center", "Yellow Zone")],
        ["location_id", "borough", "zone", "service_zone"],
    )
    zones.write.format("delta").save(cfg.table("gold/dim_zone"))

    bronze.load_month(spark_delta, cfg, "2024-01", write_month(tmp_path, "jan", jan_rows()))
    silver.build_month(spark_delta, cfg, "2024-01")
    gold.build_month(spark_delta, cfg, "2024-01")

    by_zone = spark_delta.read.format("delta").load(cfg.table("gold/trips_by_zone_day"))
    assert by_zone.agg(F.sum("trips")).first()[0] == 2
    tips = spark_delta.read.format("delta").load(cfg.table("gold/tip_rate_by_borough")).first()
    assert tips["borough"] == "Manhattan" and tips["card_trips"] == 2
