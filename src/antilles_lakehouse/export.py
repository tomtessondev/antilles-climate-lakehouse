"""Export: the gold tables as one small JSON document, `climate.json`, for the portfolio
site's climate explorer (contract in antilles-data-pipeline's
`docs/superpowers/specs/2026-11-21-explorers-design.md`).

- `territory_anomalies`: per territory, year and month, the mean of the station anomalies
  and, for each of Tx and rain, how many stations it is the mean of (`tx_stations`,
  `rr_stations`: all stations with a normal and that month complete, not the panel).
- `station_year_anomalies`: per station and year, the mean Tx anomaly of its complete
  months, kept only with at least 10 of them: fewer would make a season pass for a year.
- Anomalies and means are rounded to 0.01; dates are ISO strings.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as func

from antilles_lakehouse.gold import NORMAL_FROM, NORMAL_TO

MIN_MONTHS = 10
FILE_NAME = "climate.json"


def _round(column: str) -> Column:
    return func.round(func.col(column), 2).alias(column)


def _rows(frame: DataFrame) -> list[dict[str, Any]]:
    return [row.asDict() for row in frame.collect()]


def _stations(spark: SparkSession, stations: str) -> DataFrame:
    return spark.table(stations).select(
        "station_id",
        "name",
        "territory",
        "lat",
        "lon",
        "altitude_m",
        func.year("first_date").alias("first_year"),
        func.year("last_date").alias("last_year"),
    )


def _territory_anomalies(anomalies: DataFrame) -> DataFrame:
    return (
        anomalies.groupBy("territory", "year", "month")
        .agg(
            func.avg("tx_anomaly").alias("tx_anomaly"),
            func.avg("rr_anomaly").alias("rr_anomaly"),
            func.count("tx_anomaly").alias("tx_stations"),
            func.count("rr_anomaly").alias("rr_stations"),
        )
        .select(
            "territory",
            "year",
            "month",
            _round("tx_anomaly"),
            _round("rr_anomaly"),
            "tx_stations",
            "rr_stations",
        )
    )


def _station_year_anomalies(anomalies: DataFrame) -> DataFrame:
    return (
        anomalies.filter(func.col("tx_anomaly").isNotNull())
        .groupBy("station_id", "year")
        .agg(func.avg("tx_anomaly").alias("tx_anomaly"), func.count("*").alias("months"))
        .filter(func.col("months") >= MIN_MONTHS)
        .select("station_id", "year", _round("tx_anomaly"), "months")
    )


def _extremes(spark: SparkSession, prefix: str) -> DataFrame:
    return spark.table(f"{prefix}_extremes").select(
        "territory", "year", "indicator", "panel_size", _round("mean_per_station")
    )


def _records(spark: SparkSession, prefix: str) -> DataFrame:
    return spark.table(f"{prefix}_records").select(
        "station_id",
        "territory",
        "indicator",
        _round("value"),
        func.date_format("date", "yyyy-MM-dd").alias("date"),
    )


def build_export(
    spark: SparkSession, stations: str, prefix: str, now: datetime | None = None
) -> dict[str, Any]:
    anomalies = spark.table(f"{prefix}_anomalies")
    generated_at = (now or datetime.now(UTC)).astimezone(UTC)
    return {
        "generated_at": generated_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "normal_period": f"{NORMAL_FROM}-{NORMAL_TO}",
        "stations": _rows(_stations(spark, stations).orderBy("station_id")),
        "territory_anomalies": _rows(
            _territory_anomalies(anomalies).orderBy("territory", "year", "month")
        ),
        "station_year_anomalies": _rows(
            _station_year_anomalies(anomalies).orderBy("station_id", "year")
        ),
        "extremes": _rows(_extremes(spark, prefix).orderBy("territory", "indicator", "year")),
        "records": _rows(_records(spark, prefix).orderBy("station_id", "indicator")),
    }


class ExportError(Exception):
    """The document must not reach the site: the published file is left as it was."""


def write_export(spark: SparkSession, stations: str, prefix: str, folder: Path) -> Path:
    """Refuses, before writing anything, an export without stations or territory
    anomalies: a broken run would otherwise empty the site's explorer."""
    document = build_export(spark, stations, prefix)
    for key in ("stations", "territory_anomalies"):
        if not document[key]:
            raise ExportError(f"{key} is empty: refusing to write {FILE_NAME}")
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / FILE_NAME
    path.write_text(json.dumps(document, ensure_ascii=False, separators=(",", ":")))
    return path
