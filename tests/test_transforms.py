from datetime import datetime

import pytest
from helpers import make_trips
from pyspark.sql import functions as F

from lakehouse.transforms import (
    SILVER_SCHEMA,
    flag,
    lowercase_columns,
    split_flagged,
    standardize,
)

MONTH = "2024-01"


def reasons(df):
    return [r["reject_reason"] for r in df.select("reject_reason").collect()]


def test_clean_row_passes_and_gets_derived_columns(spark_plain):
    df = flag(make_trips(spark_plain, {}), MONTH)
    row = df.first()
    assert row["reject_reason"] is None
    assert row["trip_duration_min"] == 20.0
    assert row["pickup_month"] == "2024-01"
    assert row["pickup_hour"] == 8


@pytest.mark.parametrize(
    "override, expected",
    [
        ({"tpep_pickup_datetime": None}, "missing_timestamp"),
        ({"tpep_dropoff_datetime": datetime(2024, 1, 15, 7, 0)}, "dropoff_before_pickup"),
        (
            {
                "tpep_pickup_datetime": datetime(2023, 12, 31, 23, 0),
                "tpep_dropoff_datetime": datetime(2023, 12, 31, 23, 30),
            },
            "pickup_outside_file_month",
        ),
        ({"fare_amount": -5.0}, "negative_fare"),
        ({"trip_distance": -1.0}, "negative_distance"),
        ({"trip_distance": 900.0}, "unreasonable_distance"),
        ({"pulocationid": None}, "missing_location"),
    ],
)
def test_each_rule_rejects_with_its_reason(spark_plain, override, expected):
    df = flag(make_trips(spark_plain, override), MONTH)
    assert reasons(df) == [expected]


def test_first_matching_rule_wins(spark_plain):
    df = flag(make_trips(spark_plain, {"fare_amount": -5.0, "trip_distance": -1.0}), MONTH)
    assert reasons(df) == ["negative_fare"]


def test_exact_duplicates_are_collapsed(spark_plain):
    flagged = flag(make_trips(spark_plain, {}, {}), MONTH)
    good, bad = split_flagged(flagged)
    assert good.count() == 1
    assert bad.count() == 0


def test_trip_id_is_deterministic_and_sensitive_to_key_columns(spark_plain):
    ids = [r["trip_id"] for r in standardize(make_trips(spark_plain, {}, {}, {"fare_amount": 19.0})).collect()]
    assert ids[0] == ids[1]
    assert ids[0] != ids[2]


def test_missing_source_columns_become_null_so_old_and_new_files_share_a_schema(spark_plain):
    df = make_trips(spark_plain, {}).drop("airport_fee")  # an older file without the column
    out = standardize(df)
    assert out.schema["airport_fee"].dataType == SILVER_SCHEMA["airport_fee"]
    assert out.schema["cbd_congestion_fee"].dataType == SILVER_SCHEMA["cbd_congestion_fee"]
    row = out.first()
    assert row["airport_fee"] is None and row["cbd_congestion_fee"] is None


def test_column_case_differences_are_normalised(spark_plain):
    df = make_trips(spark_plain, {}).withColumnRenamed("airport_fee", "Airport_fee")
    assert "airport_fee" in lowercase_columns(df).columns
    assert standardize(df).first()["airport_fee"] == 0.0


def test_good_and_bad_partition_the_input(spark_plain):
    flagged = flag(
        make_trips(spark_plain, {}, {"fare_amount": -1.0, "tpep_pickup_datetime": datetime(2024, 1, 2, 1, 0),
                                     "tpep_dropoff_datetime": datetime(2024, 1, 2, 1, 5)}),
        MONTH,
    )
    good, bad = split_flagged(flagged)
    assert good.count() + bad.count() == 2
    assert bad.select(F.col("reject_reason")).first()[0] == "negative_fare"
