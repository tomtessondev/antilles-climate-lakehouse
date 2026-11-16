"""Entry points of the job's four tasks: `antilles-lakehouse <step> [--local DIR]`.

On Databricks each task runs one step on the platform's Spark, against Unity Catalog. With
`--local DIR` the same steps run on a local Spark, tables and files under DIR. The ingest
step writes a manifest of the landed files so that bronze can run as a separate task.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import httpx
from pyspark.sql import SparkSession

from antilles_lakehouse.bronze import load_bronze
from antilles_lakehouse.gold import build_gold
from antilles_lakehouse.silver import build_silver
from antilles_lakehouse.sources import LandedFile, Resource, climate_resources, fetch_dataset, land
from antilles_lakehouse.tables import Tables

STEPS = ("ingest", "bronze", "silver", "gold")


def _session(local: Path | None) -> SparkSession:
    if local is None:
        return SparkSession.builder.getOrCreate()  # the session Databricks provides
    from antilles_lakehouse.spark import local_session  # needs the "local" extra

    return local_session(local / "warehouse", metastore=local / "metastore")


def _prepare(spark: SparkSession, tables: Tables) -> None:
    """Unity Catalog objects the job writes to, created on its first run."""
    if tables.catalog is None:
        return
    name = f"{tables.catalog}.{tables.schema}"
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {tables.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {name}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {name}.landing")


def ingest(spark: SparkSession, tables: Tables) -> None:
    _prepare(spark, tables)
    with httpx.Client() as client:
        landed = land(climate_resources(fetch_dataset(client)), tables.landing, client)
    manifest = [
        {
            "resource": asdict(f.resource)
            | {"last_modified": f.resource.last_modified.isoformat()},
            "path": str(f.path),
            "sha1": f.sha1,
        }
        for f in landed
    ]
    (tables.landing / "manifest.json").write_text(json.dumps(manifest, indent=1))


def _landed(tables: Tables) -> list[LandedFile]:
    entries = json.loads((tables.landing / "manifest.json").read_text())
    return [
        LandedFile(
            Resource(
                **(
                    e["resource"]
                    | {"last_modified": datetime.fromisoformat(e["resource"]["last_modified"])}
                )
            ),
            Path(e["path"]),
            e["sha1"],
        )
        for e in entries
    ]


def run(step: str, spark: SparkSession, tables: Tables) -> None:
    if step == "ingest":
        ingest(spark, tables)
    elif step == "bronze":
        load_bronze(spark, _landed(tables), tables.bronze)
    elif step == "silver":
        build_silver(spark, tables.bronze, tables.silver_daily, tables.silver_stations)
    else:
        build_gold(spark, tables.silver_daily, tables.silver_stations, tables.gold_prefix)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="antilles-lakehouse")
    parser.add_argument("step", choices=STEPS)
    parser.add_argument("--local", type=Path, help="run on a local Spark, under this folder")
    args = parser.parse_args(argv)
    tables = Tables.local(args.local) if args.local else Tables.unity()
    run(args.step, _session(args.local), tables)
