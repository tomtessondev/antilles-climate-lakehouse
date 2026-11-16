"""Silver: one typed row per station and day, and a dimension of the stations.

- A day can appear in two files when a period rolls over (the old "latest" file and the new
  "previous" one): the row of the most recently modified file wins.
- Values outside physical bounds are nulled; Météo-France's quality code is kept
  (0 protected, 1 validated, 9 filtered, 2 doubtful).
- The table is updated with a Delta MERGE that rewrites a row only when a value or a
  quality code changed: a correction published by Météo-France flows through, an unchanged
  republication writes nothing.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as func

# Source column -> (silver column, lower bound, upper bound).
MEASURES = {
    "RR": ("rr_mm", 0.0, 1500.0),
    "TN": ("tn_c", -10.0, 50.0),
    "TX": ("tx_c", -10.0, 50.0),
    "TM": ("tm_c", -10.0, 50.0),
    "FFM": ("ffm_ms", 0.0, 100.0),
    "FXI": ("fxi_ms", 0.0, 100.0),
}
QUALITY = {source: f"q_{name.split('_')[0]}" for source, (name, _, _) in MEASURES.items()}
COMPARED = [name for name, _, _ in MEASURES.values()] + list(QUALITY.values())

DAILY_SCHEMA = (
    "station_id STRING, date DATE, "
    + ", ".join(f"{name} DOUBLE" for name, _, _ in MEASURES.values())
    + ", "
    + ", ".join(f"{q} INT" for q in QUALITY.values())
    + ", source_resource STRING, source_last_modified TIMESTAMP"
)


def territory(station_id: Column) -> Column:
    """The 971 files also cover Saint-Barthélemy and Saint-Martin, over 200 km north of
    Guadeloupe; the station id starts with the commune's INSEE code."""
    return (
        func.when(station_id.startswith("97123"), "Saint-Barthelemy")
        .when(station_id.startswith("97127"), "Saint-Martin")
        .when(station_id.startswith("971"), "Guadeloupe")
        .otherwise("Martinique")
    )


def _latest_rows(bronze: DataFrame) -> DataFrame:
    newest_first = Window.partitionBy("NUM_POSTE", "AAAAMMJJ").orderBy(
        func.col("last_modified").desc()
    )
    return (
        bronze.withColumn("rank", func.row_number().over(newest_first))
        .filter("rank = 1")
        .drop("rank")
    )


def _typed(rows: DataFrame) -> DataFrame:
    columns = [
        func.col("NUM_POSTE").alias("station_id"),
        func.to_date("AAAAMMJJ", "yyyyMMdd").alias("date"),
    ]
    for source, (name, low, high) in MEASURES.items():
        value = func.col(source).cast("double")
        columns.append(func.when(value.between(low, high), value).alias(name))
    for source, quality in QUALITY.items():
        columns.append(func.col(f"Q{source}").cast("int").alias(quality))
    columns += [
        func.col("resource_id").alias("source_resource"),
        func.col("last_modified").alias("source_last_modified"),
    ]
    return rows.select(*columns)


def _stations(rows: DataFrame) -> DataFrame:
    latest_first = Window.partitionBy("NUM_POSTE").orderBy(func.col("AAAAMMJJ").desc())
    described = (
        rows.withColumn("rank", func.row_number().over(latest_first))
        .filter("rank = 1")
        .select(
            func.col("NUM_POSTE").alias("station_id"),
            func.col("NOM_USUEL").alias("name"),
            func.col("LAT").cast("double").alias("lat"),
            func.col("LON").cast("double").alias("lon"),
            func.col("ALTI").cast("double").alias("altitude_m"),
        )
    )
    span = rows.groupBy(func.col("NUM_POSTE").alias("station_id")).agg(
        func.min(func.to_date("AAAAMMJJ", "yyyyMMdd")).alias("first_date"),
        func.max(func.to_date("AAAAMMJJ", "yyyyMMdd")).alias("last_date"),
    )
    return described.join(span, "station_id").withColumn(
        "territory", territory(func.col("station_id"))
    )


def build_silver(spark: SparkSession, bronze: str, daily: str, stations: str) -> None:
    rows = _latest_rows(spark.table(bronze))

    schema = spark.createDataFrame([], DAILY_SCHEMA).schema
    DeltaTable.createIfNotExists(spark).tableName(daily).addColumns(schema).execute()
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in COMPARED)
    (
        DeltaTable.forName(spark, daily)
        .alias("t")
        .merge(_typed(rows).alias("s"), "t.station_id = s.station_id AND t.date = s.date")
        .whenMatchedUpdateAll(condition=changed)
        .whenNotMatchedInsertAll()
        .execute()
    )

    _stations(rows).write.format("delta").mode("overwrite").saveAsTable(stations)
