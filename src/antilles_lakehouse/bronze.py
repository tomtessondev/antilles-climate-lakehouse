"""Bronze: the rows exactly as Météo-France published them, every column as text.

Météo-France rewrites its files in place, the whole 1950-onward history included, so bronze
keeps one current copy per file rather than appending each republication: a file whose
bytes did not change is skipped, a changed one replaces its own rows. Delta's history keeps
the copies it replaced (time travel), so nothing published is lost.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as func

from antilles_lakehouse.sources import LandedFile


def _stored_fingerprints(spark: SparkSession, table: str) -> dict[str, str]:
    if not spark.catalog.tableExists(table):
        return {}
    rows = spark.table(table).select("resource_id", "sha1").distinct().collect()
    return {r.resource_id: r.sha1 for r in rows}


def _read(spark: SparkSession, file: LandedFile, ingested_at: datetime) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("inferSchema", False)
        .csv(str(file.path))
        .withColumn("resource_id", func.lit(file.resource.id))
        .withColumn("resource_title", func.lit(file.resource.title))
        .withColumn("last_modified", func.lit(file.resource.last_modified).cast("timestamp"))
        .withColumn("sha1", func.lit(file.sha1))
        .withColumn("ingested_at", func.lit(ingested_at).cast("timestamp"))
    )


def load_bronze(spark: SparkSession, files: list[LandedFile], table: str) -> list[str]:
    """Returns the ids of the files actually (re)loaded."""
    stored = _stored_fingerprints(spark, table)
    ingested_at = datetime.now(UTC)
    loaded = []
    for file in files:
        resource_id = file.resource.id
        if stored.get(resource_id) == file.sha1:
            continue
        rows = _read(spark, file, ingested_at).write.format("delta")
        if not spark.catalog.tableExists(table):
            rows.saveAsTable(table)
        elif resource_id in stored:
            rows.mode("overwrite").option("replaceWhere", f"resource_id = '{resource_id}'")
            rows.saveAsTable(table)
        else:
            rows.mode("append").saveAsTable(table)
        loaded.append(resource_id)
    return loaded
