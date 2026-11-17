"""Gold: the climate indicators the story is told with.

- Values Météo-France flags as doubtful (quality code 2) stay in silver but never reach
  gold: not in a mean, a count, a normal or a record.
- The normal is the 1991-2020 mean per station and calendar month, kept only where at least
  20 of the 30 years have that month complete (80 % of its days with a value).
- Territory figures come from a fixed panel of long-series stations (80 % of days present
  in every decade from 1991 to the last complete year): stations open and close over the
  decades, and summing whatever stations exist would show a trend made by the network.
"""

from __future__ import annotations

from datetime import date
from functools import reduce
from operator import and_

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as func

VERY_HOT_C = 32.0
HEAVY_RAIN_MM = 50.0
NORMAL_FROM, NORMAL_TO = 1991, 2020
NORMAL_MIN_YEARS = 20
COMPLETE = 0.8
DECADES = [(1991, 2000), (2001, 2010), (2011, 2020)]


def _trusted(value: str, quality: str) -> Column:
    return func.when(func.coalesce(func.col(quality), func.lit(0)) != 2, func.col(value))


def _clean(spark: SparkSession, daily: str, stations: str) -> DataFrame:
    return (
        spark.table(daily)
        .join(spark.table(stations).select("station_id", "territory"), "station_id")
        .select(
            "station_id",
            "territory",
            "date",
            func.year("date").alias("year"),
            func.month("date").alias("month"),
            _trusted("tx_c", "q_tx").alias("tx_c"),
            _trusted("tn_c", "q_tn").alias("tn_c"),
            _trusted("rr_mm", "q_rr").alias("rr_mm"),
            _trusted("fxi_ms", "q_fxi").alias("fxi_ms"),
        )
    )


def _monthly(clean: DataFrame) -> DataFrame:
    days_in_month = func.dayofmonth(func.last_day(func.min("date")))
    return clean.groupBy("station_id", "territory", "year", "month").agg(
        func.count("*").alias("days"),
        func.avg("tx_c").alias("tx_mean"),
        func.avg("tn_c").alias("tn_mean"),
        func.sum("rr_mm").alias("rr_sum"),
        func.count(func.when(func.col("tx_c") >= VERY_HOT_C, 1)).alias("very_hot_days"),
        func.count(func.when(func.col("rr_mm") >= HEAVY_RAIN_MM, 1)).alias("heavy_rain_days"),
        func.count("tx_c").alias("tx_days"),
        func.count("rr_mm").alias("rr_days"),
        (func.count("tx_c") >= COMPLETE * days_in_month).alias("tx_complete"),
        (func.count("rr_mm") >= COMPLETE * days_in_month).alias("rr_complete"),
    )


def _anomalies(monthly: DataFrame) -> DataFrame:
    in_normal = func.col("year").between(NORMAL_FROM, NORMAL_TO)
    normals = (
        monthly.filter(in_normal)
        .groupBy("station_id", "month")
        .agg(
            func.avg(func.when(func.col("tx_complete"), func.col("tx_mean"))).alias("tx_normal"),
            func.count(func.when(func.col("tx_complete"), 1)).alias("tx_years"),
            func.avg(func.when(func.col("rr_complete"), func.col("rr_sum"))).alias("rr_normal"),
            func.count(func.when(func.col("rr_complete"), 1)).alias("rr_years"),
        )
    )
    tx_normal = func.when(func.col("tx_years") >= NORMAL_MIN_YEARS, func.col("tx_normal"))
    rr_normal = func.when(func.col("rr_years") >= NORMAL_MIN_YEARS, func.col("rr_normal"))
    tx_mean = func.when(func.col("tx_complete"), func.col("tx_mean"))
    rr_sum = func.when(func.col("rr_complete"), func.col("rr_sum"))
    return (
        monthly.join(normals, ["station_id", "month"])
        .select(
            "station_id",
            "territory",
            "year",
            "month",
            tx_mean.alias("tx_mean"),
            tx_normal.alias("tx_normal"),
            (tx_mean - tx_normal).alias("tx_anomaly"),
            rr_sum.alias("rr_sum"),
            rr_normal.alias("rr_normal"),
            (rr_sum - rr_normal).alias("rr_anomaly"),
        )
        .filter("tx_anomaly IS NOT NULL OR rr_anomaly IS NOT NULL")
    )


def _last_complete_year(clean: DataFrame) -> int:
    """The last year the data covers to its 31 December."""
    last: date = clean.agg(func.max("date")).collect()[0][0]
    return last.year if (last.month, last.day) == (12, 31) else last.year - 1


def _covered(value: str, start: int, end: int) -> Column:
    days = (date(end, 12, 31) - date(start, 1, 1)).days + 1
    present = func.count(func.when(func.col("year").between(start, end), func.col(value)))
    return present >= COMPLETE * days


def _panel(clean: DataFrame, value: str, last_year: int) -> DataFrame:
    """Stations with a value on 80 % of the days of every decade, 1991 to last_year."""
    periods = [*DECADES, (2021, last_year)] if last_year >= 2021 else DECADES
    coverage = reduce(and_, [_covered(value, start, end) for start, end in periods])
    stations = clean.groupBy("station_id", "territory").agg(coverage.alias("in_panel"))
    return stations.filter("in_panel").select("station_id", "territory")


def _extremes(clean: DataFrame, monthly: DataFrame, last_year: int) -> DataFrame:
    yearly = monthly.groupBy("station_id", "territory", "year").agg(
        func.sum("very_hot_days").alias("very_hot_days"),
        func.sum("heavy_rain_days").alias("heavy_rain_days"),
        func.sum("tx_days").alias("tx_days"),
        func.sum("rr_days").alias("rr_days"),
    )
    days_in_year = func.dayofyear(func.make_date(func.col("year"), func.lit(12), func.lit(31)))
    frames = []
    for indicator, value, measured in (
        ("very_hot_days", "tx_c", "tx_days"),
        ("heavy_rain_days", "rr_mm", "rr_days"),
    ):
        panel = _panel(clean, value, last_year)
        # A panel station counts in a year only if that year is itself 80 % measured:
        # a station with three months of data would otherwise weigh as a whole year.
        frames.append(
            yearly.join(panel, ["station_id", "territory"])
            .filter(func.col("year").between(NORMAL_FROM, last_year))
            .filter(func.col(measured) >= COMPLETE * days_in_year)
            .groupBy("territory", "year")
            .agg(
                func.count("*").alias("panel_size"),
                func.avg(indicator).cast("double").alias("mean_per_station"),
            )
            .withColumn("indicator", func.lit(indicator))
        )
    return (
        frames[0]
        .unionByName(frames[1])
        .select("territory", "year", "indicator", "panel_size", "mean_per_station")
    )


def _records(clean: DataFrame) -> DataFrame:
    frames = []
    for indicator, value in (
        ("max_tx_c", "tx_c"),
        ("max_rr_mm", "rr_mm"),
        ("max_fxi_ms", "fxi_ms"),
    ):
        best = func.max_by("date", value)
        frames.append(
            clean.filter(func.col(value).isNotNull())
            .groupBy("station_id", "territory")
            .agg(func.max(value).alias("value"), best.alias("date"))
            .withColumn("indicator", func.lit(indicator))
        )
    records = frames[0]
    for frame in frames[1:]:
        records = records.unionByName(frame)
    return records.select("station_id", "territory", "indicator", "value", "date")


def build_gold(spark: SparkSession, daily: str, stations: str, prefix: str) -> None:
    clean = _clean(spark, daily, stations)
    monthly = _monthly(clean)
    last_year = _last_complete_year(clean)
    tables = {
        "monthly": monthly.drop("tx_complete", "rr_complete"),
        "anomalies": _anomalies(monthly),
        "extremes": _extremes(clean, monthly, last_year),
        "records": _records(clean),
    }
    for name, frame in tables.items():
        frame.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(
            f"{prefix}_{name}"
        )
