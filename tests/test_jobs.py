"""The five job steps, run the way the Databricks job runs them, on a local Spark."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import respx
from meteo_files import row, write
from pyspark.sql import SparkSession

from antilles_lakehouse.jobs import main
from antilles_lakehouse.sources import DATASET_URL, climate_resources

FIXTURES = Path(__file__).parent / "fixtures"
# Twenty complete Januaries in 1991-2020: enough for a January normal, so the export has
# territory anomalies (it refuses to write without them).
NORMAL_DAYS = [f"{year}01{day:02d}" for year in range(1991, 2011) for day in range(1, 32)]


@respx.mock
def test_pipeline_runs_end_to_end_locally(spark: SparkSession, tmp_path: Path) -> None:
    dataset = json.loads((FIXTURES / "dataset.json").read_text())
    respx.get(DATASET_URL).mock(return_value=httpx.Response(200, json=dataset))
    for resource in climate_resources(dataset):
        station = "97101014" if resource.department == "971" else "97209001"
        day = "20250115" if resource.period == "2025-2026" else "20240115"
        rows = [row(station, day, TX="33.0")]
        if resource.period == "1950-2024":
            rows += [row(station, normal_day, TX="28.0") for normal_day in NORMAL_DAYS]
        body = write(tmp_path / "src" / f"{resource.id}.gz", rows)
        respx.get(resource.url).mock(return_value=httpx.Response(200, content=body.read_bytes()))

    root = tmp_path / "lake"
    for step in ("ingest", "bronze", "silver", "gold", "export"):
        main([step, "--local", str(root)])

    manifest = json.loads((root / "landing" / "manifest.json").read_text())
    assert len(manifest) == 4
    assert spark.table("silver_daily").count() == 4 + 2 * len(NORMAL_DAYS)
    assert spark.table("gold_monthly").filter("very_hot_days = 1").count() == 4
    assert spark.table("gold_records").count() > 0
    exported = json.loads((root / "exports" / "climate.json").read_text())
    assert {s["station_id"] for s in exported["stations"]} == {"97101014", "97209001"}
    assert {t["territory"] for t in exported["territory_anomalies"]} == {
        "Guadeloupe",
        "Martinique",
    }
    assert len(exported["records"]) == spark.table("gold_records").count()
