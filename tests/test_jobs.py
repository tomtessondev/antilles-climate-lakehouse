"""The four job steps, run the way the Databricks job runs them, on a local Spark."""

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


@respx.mock
def test_pipeline_runs_end_to_end_locally(spark: SparkSession, tmp_path: Path) -> None:
    dataset = json.loads((FIXTURES / "dataset.json").read_text())
    respx.get(DATASET_URL).mock(return_value=httpx.Response(200, json=dataset))
    for resource in climate_resources(dataset):
        station = "97101014" if resource.department == "971" else "97209001"
        day = "20250115" if resource.period == "2025-2026" else "20240115"
        body = write(tmp_path / "src" / f"{resource.id}.gz", [row(station, day, TX="33.0")])
        respx.get(resource.url).mock(return_value=httpx.Response(200, content=body.read_bytes()))

    root = tmp_path / "lake"
    for step in ("ingest", "bronze", "silver", "gold"):
        main([step, "--local", str(root)])

    manifest = json.loads((root / "landing" / "manifest.json").read_text())
    assert len(manifest) == 4
    assert spark.table("silver_daily").count() == 4
    assert spark.table("gold_monthly").filter("very_hot_days = 1").count() == 4
    assert spark.table("gold_records").count() > 0
