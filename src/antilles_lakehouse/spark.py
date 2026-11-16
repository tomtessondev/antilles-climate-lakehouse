"""A local Spark session with Delta Lake, for the tests and `make run-local`. On Databricks
the job uses the session the platform provides; the layers only ever receive a session."""

from __future__ import annotations

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def local_session(warehouse: Path) -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("antilles-climate-lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
        # Dates are calendar days in the Antilles' records; timestamps are written in UTC.
        .config("spark.sql.session.timeZone", "UTC")
        # Small data in tests: the default 200 shuffle partitions would only slow them down.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    session: SparkSession = configure_spark_with_delta_pip(builder).getOrCreate()
    return session
