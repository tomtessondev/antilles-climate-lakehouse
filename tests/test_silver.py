"""Silver: typed daily observations, one row per station and day, merged so that only what
Météo-France actually changed is rewritten."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from meteo_files import landed, row, write
from pyspark.sql import SparkSession

from antilles_lakehouse.bronze import load_bronze
from antilles_lakehouse.silver import build_silver
from antilles_lakehouse.sources import LandedFile

LATER = datetime(2026, 11, 15, tzinfo=UTC)


def last_merge(spark: SparkSession, table: str) -> dict[str, str]:
    history = spark.sql(f"DESCRIBE HISTORY {table}").orderBy("version", ascending=False)
    metrics: dict[str, str] = history.first()["operationMetrics"]
    return metrics


def build(spark: SparkSession, name: str, files: list[LandedFile]) -> None:
    load_bronze(spark, files, f"{name}_bronze")
    build_silver(spark, f"{name}_bronze", f"{name}_daily", f"{name}_stations")


def test_types_and_bounds(spark: SparkSession, tmp_path: Path) -> None:
    rows = [row("97101014", "20250101", RR="0.8", QRR="1", TX="60.5", QTX="2", TN="20.6")]
    build(spark, "s_types", [landed(write(tmp_path / "a.csv.gz", rows), "a")])

    (day,) = spark.table("s_types_daily").collect()
    assert day.station_id == "97101014" and day.date == date(2025, 1, 1)
    assert day.rr_mm == 0.8 and day.tn_c == 20.6
    assert day.tx_c is None  # 60.5 °C is outside the physical bounds
    assert day.q_tx == 2  # the quality code is kept


def test_period_rollover_keeps_one_row_from_the_newer_file(
    spark: SparkSession, tmp_path: Path
) -> None:
    old_latest = landed(
        write(tmp_path / "l.csv.gz", [row("97101014", "20241231", TX="29.0")]),
        "latest",
        "2024-2025",
    )
    new_previous = landed(
        write(tmp_path / "p.csv.gz", [row("97101014", "20241231", TX="29.4")]),
        "previous",
        "1950-2024",
        modified=LATER,
    )
    build(spark, "s_rollover", [old_latest, new_previous])

    (day,) = spark.table("s_rollover_daily").collect()
    assert day.tx_c == 29.4 and day.source_resource == "previous"


def test_correction_updates_only_that_row(spark: SparkSession, tmp_path: Path) -> None:
    days = ["20250101", "20250102", "20250103"]
    first = [row("97101014", d, TX="29.0") for d in days]
    build(spark, "s_fix", [landed(write(tmp_path / "v1.csv.gz", first), "a")])

    corrected = [row("97101014", d, TX="31.0" if d == "20250102" else "29.0") for d in days]
    build(spark, "s_fix", [landed(write(tmp_path / "v2.csv.gz", corrected), "a", modified=LATER)])

    metrics = last_merge(spark, "s_fix_daily")
    assert metrics["numTargetRowsUpdated"] == "1"
    assert metrics["numTargetRowsInserted"] == "0"
    assert spark.table("s_fix_daily").filter("date = '2025-01-02'").first().tx_c == 31.0


def test_unchanged_republication_updates_nothing(spark: SparkSession, tmp_path: Path) -> None:
    rows = [row("97101014", "20250101", TX="29.0"), row("97209001", "20250101", TX="30.0")]
    build(spark, "s_same", [landed(write(tmp_path / "v1.csv.gz", rows), "a")])
    # Same values, new bytes (Météo-France rewrote the file): bronze reloads it, silver
    # finds nothing to change.
    rows_again = rows + [row("97101014", "20250101", TX="29.0")]
    build(spark, "s_same", [landed(write(tmp_path / "v2.csv.gz", rows_again), "a", modified=LATER)])

    metrics = last_merge(spark, "s_same_daily")
    assert metrics["numTargetRowsUpdated"] == "0"
    assert metrics["numTargetRowsInserted"] == "0"


def test_territories(spark: SparkSession, tmp_path: Path) -> None:
    stations = ["97101014", "97123001", "97127002", "97209001"]
    rows = [row(s, "20250101", TX="29.0") for s in stations]
    build(spark, "s_terr", [landed(write(tmp_path / "a.csv.gz", rows), "a")])

    territory = {r.station_id: r.territory for r in spark.table("s_terr_stations").collect()}
    assert territory == {
        "97101014": "Guadeloupe",
        "97123001": "Saint-Barthelemy",
        "97127002": "Saint-Martin",
        "97209001": "Martinique",
    }


def test_previous_version_is_still_readable(spark: SparkSession, tmp_path: Path) -> None:
    build(
        spark,
        "s_tt",
        [landed(write(tmp_path / "v1.csv.gz", [row("97101014", "20250101", TX="29.0")]), "a")],
    )
    before = spark.sql("DESCRIBE HISTORY s_tt_daily").agg({"version": "max"}).first()[0]
    build(
        spark,
        "s_tt",
        [
            landed(
                write(tmp_path / "v2.csv.gz", [row("97101014", "20250101", TX="33.0")]),
                "a",
                modified=LATER,
            )
        ],
    )
    assert spark.table("s_tt_daily").first().tx_c == 33.0
    assert spark.read.option("versionAsOf", before).table("s_tt_daily").first().tx_c == 29.0
