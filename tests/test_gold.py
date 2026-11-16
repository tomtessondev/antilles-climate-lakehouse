"""Gold: monthly climate, anomalies against 1991-2020, extremes on a fixed panel of
long-series stations, records. Silver tables are built directly for each scenario."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as func

from antilles_lakehouse.gold import build_gold
from antilles_lakehouse.silver import territory


def days(
    spark: SparkSession,
    station: str,
    start: str,
    end: str,
    tx: float = 29.0,
    rr: float = 2.0,
    q_tx: int = 1,
) -> DataFrame:
    """One silver row per day from start to end, constant values."""
    dates = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}')) AS date")
    return dates.select(
        func.lit(station).alias("station_id"),
        "date",
        func.lit(rr).alias("rr_mm"),
        func.lit(tx - 7).alias("tn_c"),
        func.lit(tx).alias("tx_c"),
        func.lit(tx - 3).alias("tm_c"),
        func.lit(4.0).alias("ffm_ms"),
        func.lit(10.0).alias("fxi_ms"),
        func.lit(1).alias("q_rr"),
        func.lit(1).alias("q_tn"),
        func.lit(q_tx).alias("q_tx"),
        func.lit(1).alias("q_tm"),
        func.lit(1).alias("q_ffm"),
        func.lit(1).alias("q_fxi"),
        func.lit("r").alias("source_resource"),
        func.current_timestamp().alias("source_last_modified"),
    )


def build(spark: SparkSession, name: str, *frames: DataFrame) -> None:
    daily = frames[0]
    for frame in frames[1:]:
        daily = daily.unionByName(frame)
    daily.write.format("delta").mode("overwrite").saveAsTable(f"{name}_daily")
    stations = daily.groupBy("station_id").agg(
        func.min("date").alias("first_date"), func.max("date").alias("last_date")
    )
    stations.withColumn("territory", territory(func.col("station_id"))).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(f"{name}_stations")
    build_gold(spark, f"{name}_daily", f"{name}_stations", f"{name}_gold")


def test_counts_very_hot_and_heavy_rain_days(spark: SparkSession) -> None:
    build(
        spark,
        "g_counts",
        days(spark, "97101014", "2025-01-01", "2025-01-01", tx=32.0, rr=50.0),
        days(spark, "97101014", "2025-01-02", "2025-01-02", tx=31.9, rr=49.9),
    )
    (month,) = spark.table("g_counts_gold_monthly").collect()
    assert month.very_hot_days == 1 and month.heavy_rain_days == 1


def test_doubtful_values_stay_out_of_gold(spark: SparkSession) -> None:
    build(
        spark,
        "g_doubt",
        days(spark, "97101014", "2025-01-01", "2025-01-01", tx=40.0, q_tx=2),
        days(spark, "97101014", "2025-01-02", "2025-01-02", tx=30.0),
    )
    assert spark.table("g_doubt_gold_monthly").first().very_hot_days == 0
    record = spark.table("g_doubt_gold_records").filter("indicator = 'max_tx_c'").first()
    assert record.value == 30.0


def test_incomplete_months_make_no_normal(spark: SparkSession) -> None:
    years = [
        days(spark, "97101014", f"{y}-01-01", f"{y}-01-10")  # 10 January days: incomplete
        for y in range(1991, 2021)
    ] + [days(spark, "97101014", f"{y}-02-01", f"{y}-02-28") for y in range(1991, 2021)]
    build(spark, "g_normal", *years, days(spark, "97101014", "2025-01-01", "2025-02-28", tx=31.0))

    anomalies = spark.table("g_normal_gold_anomalies").filter("year = 2025")
    assert {r.month for r in anomalies.collect()} == {2}


def test_anomaly_is_month_mean_minus_normal(spark: SparkSession) -> None:
    build(
        spark,
        "g_anomaly",
        days(spark, "97209001", "1991-01-01", "2020-12-31", tx=30.0),
        days(spark, "97209001", "2025-01-01", "2025-01-31", tx=31.0),
    )
    row = spark.table("g_anomaly_gold_anomalies").filter("year = 2025 AND month = 1").first()
    assert row.tx_normal == 30.0 and row.tx_anomaly == 1.0


def test_station_opening_late_is_not_in_the_panel(spark: SparkSession) -> None:
    build(
        spark,
        "g_panel",
        days(spark, "97101014", "1991-01-01", "2025-12-31", tx=29.0),
        days(spark, "97110001", "2015-01-01", "2025-12-31", tx=33.0),  # opens in 2015
    )
    hot = spark.table("g_panel_gold_extremes").filter(
        "territory = 'Guadeloupe' AND indicator = 'very_hot_days' AND year = 2025"
    )
    (row,) = hot.collect()
    assert row.panel_size == 1
    assert row.mean_per_station == 0.0  # the long-series station never reaches 32 °C


def test_records_keep_the_date(spark: SparkSession) -> None:
    build(
        spark,
        "g_records",
        days(spark, "97209001", "2025-01-01", "2025-01-31", rr=10.0),
        days(spark, "97209001", "2025-02-01", "2025-02-01", rr=210.5),
    )
    record = spark.table("g_records_gold_records").filter("indicator = 'max_rr_mm'").first()
    assert record.value == 210.5 and record.date == date(2025, 2, 1)
