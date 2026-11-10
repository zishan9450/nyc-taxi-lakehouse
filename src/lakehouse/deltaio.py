from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def replace_partition(
    spark: SparkSession, df: DataFrame, path: str, partition_col: str, value: str
) -> None:
    """Idempotently (re)write one partition of a Delta table.

    Re-running a month replaces that month's data and leaves every other month untouched.
    mergeSchema lets new upstream columns appear without breaking the load.
    """
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy(partition_col)
    )
    if DeltaTable.isDeltaTable(spark, path):
        writer = writer.option("replaceWhere", f"{partition_col} = '{value}'")
    writer.save(path)
