"""Export: climate.json, the contract the portfolio site validates. Gold and silver tables
are written directly for each scenario."""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from antilles_lakehouse.export import build_export, write_export

ANOMALIES = (
    "station_id STRING, territory STRING, year INT, month INT, tx_mean DOUBLE, "
    "tx_normal DOUBLE, tx_anomaly DOUBLE, rr_sum DOUBLE, rr_normal DOUBLE, rr_anomaly DOUBLE"
)
STATIONS = (
    "station_id STRING, name STRING, lat DOUBLE, lon DOUBLE, altitude_m DOUBLE, "
    "first_date DATE, last_date DATE, territory STRING"
)
EXTREMES = (
    "territory STRING, year INT, indicator STRING, panel_size BIGINT, mean_per_station DOUBLE"
)
RECORDS = "station_id STRING, territory STRING, indicator STRING, value DOUBLE, date DATE"

GUADELOUPE = ("97101014", "Le Raizet", 16.26, -61.51, 11.0, date(1951, 1, 1), date(2025, 12, 31))
MARTINIQUE = ("97209001", "Lamentin", 14.6, -61.0, 3.0, date(1991, 6, 1), date(2025, 12, 31))


def anomaly(
    station: str, territory: str, year: int, month: int, tx: float | None, rr: float | None
) -> tuple[object, ...]:
    return (station, territory, year, month, None, None, tx, None, None, rr)


def build(
    spark: SparkSession,
    name: str,
    anomalies: list[tuple[object, ...]],
    extremes: list[tuple[object, ...]] | None = None,
    records: list[tuple[object, ...]] | None = None,
) -> None:
    stations = [(*GUADELOUPE, "Guadeloupe"), (*MARTINIQUE, "Martinique")]
    for rows, schema, table in (
        (stations, STATIONS, f"{name}_stations"),
        (anomalies, ANOMALIES, f"{name}_gold_anomalies"),
        (extremes or [], EXTREMES, f"{name}_gold_extremes"),
        (records or [], RECORDS, f"{name}_gold_records"),
    ):
        spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").saveAsTable(
            table
        )


def export(spark: SparkSession, name: str) -> dict[str, Any]:
    now = datetime(2026, 11, 2, 14, 0, 5, tzinfo=UTC)
    return build_export(spark, f"{name}_stations", f"{name}_gold", now=now)


def test_document_follows_the_contract(spark: SparkSession, tmp_path: Path) -> None:
    build(
        spark,
        "x_shape",
        [anomaly("97101014", "Guadeloupe", 2025, m, 1.0, 10.0) for m in range(1, 13)],
        [("Guadeloupe", 2025, "very_hot_days", 2, 12.5)],
        [("97209001", "Martinique", "max_rr_mm", 210.5, date(2025, 2, 1))],
    )
    path = write_export(spark, "x_shape_stations", "x_shape_gold", tmp_path / "exports")
    document = json.loads(path.read_text())

    assert path.name == "climate.json"
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", document["generated_at"])
    assert document["normal_period"] == "1991-2020"
    assert document["stations"][0] == {
        "station_id": "97101014",
        "name": "Le Raizet",
        "territory": "Guadeloupe",
        "lat": 16.26,
        "lon": -61.51,
        "altitude_m": 11.0,
        "first_year": 1951,
        "last_year": 2025,
    }
    assert document["territory_anomalies"][0] == {
        "territory": "Guadeloupe",
        "year": 2025,
        "month": 1,
        "tx_anomaly": 1.0,
        "rr_anomaly": 10.0,
        "stations": 1,
    }
    assert document["station_year_anomalies"] == [
        {"station_id": "97101014", "year": 2025, "tx_anomaly": 1.0, "months": 12}
    ]
    assert document["extremes"] == [
        {
            "territory": "Guadeloupe",
            "year": 2025,
            "indicator": "very_hot_days",
            "panel_size": 2,
            "mean_per_station": 12.5,
        }
    ]
    assert document["records"] == [
        {
            "station_id": "97209001",
            "territory": "Martinique",
            "indicator": "max_rr_mm",
            "value": 210.5,
            "date": "2025-02-01",
        }
    ]


def test_generated_at_is_utc(spark: SparkSession) -> None:
    build(spark, "x_time", [])
    assert export(spark, "x_time")["generated_at"] == "2026-11-02T14:00:05Z"


def test_territory_month_is_the_mean_of_its_stations(spark: SparkSession) -> None:
    build(
        spark,
        "x_territory",
        [
            anomaly("97101014", "Guadeloupe", 2025, 1, 1.0, 20.0),
            anomaly("97110001", "Guadeloupe", 2025, 1, 2.0, None),  # rain incomplete
            anomaly("97209001", "Martinique", 2025, 1, -0.5, -4.0),
        ],
    )
    rows = export(spark, "x_territory")["territory_anomalies"]
    assert rows == [
        {
            "territory": "Guadeloupe",
            "year": 2025,
            "month": 1,
            "tx_anomaly": 1.5,
            "rr_anomaly": 20.0,
            "stations": 2,
        },
        {
            "territory": "Martinique",
            "year": 2025,
            "month": 1,
            "tx_anomaly": -0.5,
            "rr_anomaly": -4.0,
            "stations": 1,
        },
    ]


def test_station_year_needs_ten_complete_months(spark: SparkSession) -> None:
    rows = [anomaly("97101014", "Guadeloupe", 2024, m, 1.0, None) for m in range(1, 11)]
    rows += [anomaly("97101014", "Guadeloupe", 2024, m, None, 5.0) for m in (11, 12)]
    # Nine months with a temperature anomaly, the rest only rain: not a year.
    rows += [anomaly("97209001", "Martinique", 2024, m, 0.5, 1.0) for m in range(1, 10)]
    rows += [anomaly("97209001", "Martinique", 2024, m, None, 1.0) for m in (10, 11, 12)]
    build(spark, "x_months", rows)

    assert export(spark, "x_months")["station_year_anomalies"] == [
        {"station_id": "97101014", "year": 2024, "tx_anomaly": 1.0, "months": 10}
    ]


def test_values_are_rounded_to_a_hundredth(spark: SparkSession) -> None:
    rows = [anomaly("97101014", "Guadeloupe", 2025, m, 1.0 / 3, 12.345678) for m in range(1, 13)]
    build(
        spark,
        "x_round",
        rows,
        [("Guadeloupe", 2025, "heavy_rain_days", 3, 2.0 / 3)],
    )
    document = export(spark, "x_round")

    month = document["territory_anomalies"][0]
    assert (month["tx_anomaly"], month["rr_anomaly"]) == (0.33, 12.35)
    assert document["station_year_anomalies"][0]["tx_anomaly"] == 0.33
    assert document["extremes"][0]["mean_per_station"] == 0.67
