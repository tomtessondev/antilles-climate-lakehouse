"""The local Spark session writes Delta tables and keeps their history."""

from __future__ import annotations

from pyspark.sql import SparkSession


def test_delta_round_trip_and_time_travel(spark: SparkSession) -> None:
    spark.createDataFrame([(1,), (2,)], "n int").write.format("delta").saveAsTable("t")
    spark.createDataFrame([(3,)], "n int").write.format("delta").mode("overwrite").saveAsTable("t")
    assert spark.table("t").count() == 1
    assert spark.read.option("versionAsOf", 0).table("t").count() == 2
