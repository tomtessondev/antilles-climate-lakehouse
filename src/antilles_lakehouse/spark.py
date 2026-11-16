"""A local Spark session with Delta Lake, for the tests and `make run-local`. On Databricks
the job uses the session the platform provides; the layers only ever receive a session."""

from __future__ import annotations

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def local_session(warehouse: Path, metastore: Path | None = None) -> SparkSession:
    """With a metastore folder, table definitions persist between processes (each job step
    of `make run-local` is its own process); without one they live in memory (tests)."""
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
    if metastore is not None:
        builder = builder.enableHiveSupport().config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore};create=true",
        )
    session: SparkSession = configure_spark_with_delta_pip(builder).getOrCreate()
    return session
