"""Bronze keeps the rows as published, one current copy per file, and Delta's history keeps
the copies a republication replaced."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from meteo_files import HEADER, landed, row, write
from pyspark.sql import SparkSession

from antilles_lakehouse.bronze import load_bronze


def history_length(spark: SparkSession, table: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {table}").count()


def test_first_load_keeps_every_row_as_text(spark: SparkSession, tmp_path: Path) -> None:
    path = write(
        tmp_path / "a.csv.gz",
        [row("97101014", "20250101", TX="29.3", QTX="1"), row("97101014", "20250102")],
    )
    assert load_bronze(spark, [landed(path, "a")], "bronze_first") == ["a"]

    table = spark.table("bronze_first")
    assert table.count() == 2
    assert all(t == "string" for name, t in table.dtypes if name in HEADER)
    assert {r.resource_id for r in table.select("resource_id").collect()} == {"a"}
    assert table.filter("AAAAMMJJ = '20250101'").first()["TX"] == "29.3"


def test_same_file_twice_writes_nothing(spark: SparkSession, tmp_path: Path) -> None:
    file = landed(write(tmp_path / "b.csv.gz", [row("97101014", "20250101")]), "b")
    load_bronze(spark, [file], "bronze_twice")
    versions = history_length(spark, "bronze_twice")

    assert load_bronze(spark, [file], "bronze_twice") == []
    assert history_length(spark, "bronze_twice") == versions
    assert spark.table("bronze_twice").count() == 1


def test_republished_file_replaces_its_rows_and_history_keeps_the_old(
    spark: SparkSession, tmp_path: Path
) -> None:
    other = landed(write(tmp_path / "o.csv.gz", [row("97209001", "20250101")]), "o")
    first = landed(write(tmp_path / "c1.csv.gz", [row("97101014", "20250101", TX="29.3")]), "c")
    load_bronze(spark, [first, other], "bronze_republished")
    before = history_length(spark, "bronze_republished") - 1

    corrected = landed(
        write(tmp_path / "c2.csv.gz", [row("97101014", "20250101", TX="30.1")]),
        "c",
        modified=datetime(2026, 11, 15, tzinfo=UTC),
    )
    assert load_bronze(spark, [corrected, other], "bronze_republished") == ["c"]

    table = spark.table("bronze_republished")
    assert table.count() == 2  # replaced, not appended; the other file untouched
    assert table.filter("resource_id = 'c'").first()["TX"] == "30.1"
    old = spark.read.option("versionAsOf", before).table("bronze_republished")
    assert old.filter("resource_id = 'c'").first()["TX"] == "29.3"
