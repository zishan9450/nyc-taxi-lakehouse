"""Pure DataFrame transforms: no I/O, no Delta. Unit-testable with a plain Spark session."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, DoubleType, IntegerType, StringType, TimestampType

# Source column (lower-cased) -> silver column.
RENAMES = {
    "vendorid": "vendor_id",
    "ratecodeid": "ratecode_id",
    "pulocationid": "pu_location_id",
    "dolocationid": "do_location_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
}
_SOURCE_NAME = {new: raw for raw, new in RENAMES.items()}

# The silver contract. Columns missing from a source file become NULL, so older and newer
# TLC files produce the same silver schema. Brand-new upstream columns land in bronze
# automatically (mergeSchema) and are promoted to silver by adding one line here.
SILVER_SCHEMA: dict[str, DataType] = {
    "vendor_id": IntegerType(),
    "pickup_ts": TimestampType(),
    "dropoff_ts": TimestampType(),
    "passenger_count": IntegerType(),
    "trip_distance": DoubleType(),
    "ratecode_id": IntegerType(),
    "store_and_fwd_flag": StringType(),
    "pu_location_id": IntegerType(),
    "do_location_id": IntegerType(),
    "payment_type": IntegerType(),
    "fare_amount": DoubleType(),
    "extra": DoubleType(),
    "mta_tax": DoubleType(),
    "tip_amount": DoubleType(),
    "tolls_amount": DoubleType(),
    "improvement_surcharge": DoubleType(),
    "congestion_surcharge": DoubleType(),
    "airport_fee": DoubleType(),
    "cbd_congestion_fee": DoubleType(),
    "total_amount": DoubleType(),
}

# Natural key. Rows identical on all of these are treated as duplicates.
KEY_COLS = [
    "vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id",
    "trip_distance", "fare_amount", "total_amount",
]

MAX_TRIP_MILES = 500.0


def lowercase_columns(df: DataFrame) -> DataFrame:
    """TLC changed column casing over time (e.g. Airport_fee vs airport_fee)."""
    return df.toDF(*[c.lower() for c in df.columns])


def standardize(df: DataFrame) -> DataFrame:
    """Apply the silver contract: rename, cast, fill missing columns, add derived columns."""
    lowered = lowercase_columns(df)
    cols = []
    for name, dtype in SILVER_SCHEMA.items():
        source = _SOURCE_NAME.get(name, name)
        if source in lowered.columns:
            cols.append(F.col(source).cast(dtype).alias(name))
        else:
            cols.append(F.lit(None).cast(dtype).alias(name))
    out = lowered.select(*cols)

    trip_id = F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in KEY_COLS]), 256)
    return (
        out.withColumn("trip_id", trip_id)
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0,
        )
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
    )


def reject_rules(month: str) -> list[tuple[str, Column]]:
    """(reason, condition). A row is rejected with the FIRST reason whose condition is true."""
    month_start = F.lit(f"{month}-01").cast("date")
    month_end = F.add_months(month_start, 1)
    return [
        ("missing_timestamp", F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull()),
        ("dropoff_before_pickup", F.col("dropoff_ts") < F.col("pickup_ts")),
        (
            "pickup_outside_file_month",
            (F.col("pickup_ts") < month_start) | (F.col("pickup_ts") >= month_end),
        ),
        ("negative_fare", F.col("fare_amount") < 0),
        ("negative_distance", F.col("trip_distance") < 0),
        ("unreasonable_distance", F.col("trip_distance") > MAX_TRIP_MILES),
        ("missing_location", F.col("pu_location_id").isNull() | F.col("do_location_id").isNull()),
    ]


def flag(df: DataFrame, month: str) -> DataFrame:
    """Standardize and add reject_reason (NULL means the row is clean)."""
    rules = reject_rules(month)
    reason = F.coalesce(*[F.when(cond, F.lit(name)) for name, cond in rules])
    return standardize(df).withColumn("reject_reason", reason)


def split_flagged(flagged: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (good, bad). Good rows are de-duplicated on trip_id."""
    good = (
        flagged.filter(F.col("reject_reason").isNull())
        .drop("reject_reason")
        .dropDuplicates(["trip_id"])
    )
    bad = flagged.filter(F.col("reject_reason").isNotNull())
    return good, bad
