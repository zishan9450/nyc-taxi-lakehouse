"""Shared test data: a realistic default trip row plus overrides."""
from __future__ import annotations

from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA = StructType([
    StructField("vendorid", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("ratecodeid", DoubleType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("pulocationid", IntegerType()),
    StructField("dolocationid", IntegerType()),
    StructField("payment_type", LongType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("airport_fee", DoubleType()),
])

DEFAULT = {
    "vendorid": 1,
    "tpep_pickup_datetime": datetime(2024, 1, 15, 8, 0),
    "tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 20),
    "passenger_count": 1.0,
    "trip_distance": 3.2,
    "ratecodeid": 1.0,
    "store_and_fwd_flag": "N",
    "pulocationid": 161,
    "dolocationid": 237,
    "payment_type": 1,
    "fare_amount": 18.0,
    "extra": 1.0,
    "mta_tax": 0.5,
    "tip_amount": 3.0,
    "tolls_amount": 0.0,
    "improvement_surcharge": 1.0,
    "total_amount": 26.0,
    "congestion_surcharge": 2.5,
    "airport_fee": 0.0,
}


def make_rows(*overrides: dict) -> list[dict]:
    return [{**DEFAULT, **o} for o in overrides]


def make_trips(spark, *overrides: dict):
    rows = make_rows(*overrides)
    return spark.createDataFrame([tuple(r[f.name] for f in SCHEMA.fields) for r in rows], SCHEMA)
