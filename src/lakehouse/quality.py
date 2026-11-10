"""Quality gates. A failed gate raises, which fails the Airflow task and stops publishing."""
from __future__ import annotations

from pyspark.sql import DataFrame


class QualityError(RuntimeError):
    pass


def check_not_empty(rows: int, what: str) -> None:
    if rows == 0:
        raise QualityError(f"{what} has no rows")


def check_reject_rate(rejected: int, total: int, max_rate: float) -> float:
    if total == 0:
        raise QualityError("cannot compute reject rate on zero rows")
    rate = rejected / total
    if rate > max_rate:
        raise QualityError(f"reject rate {rate:.2%} exceeds allowed {max_rate:.2%}")
    return rate


def check_unique_key(df: DataFrame, key: str = "trip_id") -> None:
    total = df.count()
    distinct = df.select(key).distinct().count()
    if total != distinct:
        raise QualityError(f"{key} is not unique: {total} rows, {distinct} distinct")
