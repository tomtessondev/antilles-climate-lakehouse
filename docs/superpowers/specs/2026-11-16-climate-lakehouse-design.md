# Antilles climate lakehouse — design

## Intent

The next portfolio project from `antilles-data-pipeline/docs/ROADMAP.md` (phase 4,
"Lakehouse on Databricks"): a medallion architecture (bronze, silver, gold) in PySpark and
Delta Lake, deployed on Databricks, on public data about the French Antilles. The owner is
an FPGA / embedded engineer moving to freelance data engineering; the project must show
what Databricks missions ask for (PySpark, Delta, Unity Catalog, jobs deployed as code)
with the same standards as the other three repositories: tests, CI, clear docs, a French
page on `/projets`.

The story: **how the climate of Guadeloupe and Martinique has changed**, from Météo-France's
daily climatological series (open data): very hot days, rainfall extremes, departure from
the normal, per station and per island, over decades.

## Goals

- A new repository `antilles-climate-lakehouse` (GitHub, public, same owner), Python 3.12
  package `antilles_lakehouse`, ruff, mypy `--strict`, pytest, GitHub Actions.
- Ingestion of the daily files for departments 971 and 972 ("RR-T-Vent": rain,
  temperature, wind) from the data.gouv.fr dataset "Données climatologiques de base -
  quotidiennes" (id `6569b51ae64326786e4e8e1a`), resources found through the data.gouv.fr
  API, never hard-coded URLs.
- Three layers as Delta tables in Unity Catalog:
  - **bronze**: the CSV rows as strings plus the source resource, its `last_modified`,
    its checksum and the ingestion time (see "Republished files" below);
  - **silver**: typed daily observations per station and date, Météo-France quality codes
    kept, sentinel or impossible values nulled, upserted with Delta `MERGE` on
    (station, date); a station dimension (name, coordinates, altitude, territory);
  - **gold**: monthly climate per station; anomalies against the 1991–2020 normal; yearly
    counts of very hot days and heavy-rain days per station and per territory (see
    "Territories and a changing station network"); record values.
- The same transformation code runs locally (PySpark + delta-spark, tests and CI) and on
  Databricks (serverless job deployed by a Databricks Asset Bundle).
- Delta features shown on purpose: `MERGE`, schema enforcement, time travel (history and
  reading an older version, in a test), table maintenance (`OPTIMIZE`, liquid clustering
  where available).
- Visible results without running anything: charts from the gold tables and screenshots
  of the Databricks job and catalog in the README and on the French page.

## Non-goals

- Streaming or Auto Loader (the AIS project covers streaming; Auto Loader only runs on
  Databricks and cannot be tested in CI).
- Lakeflow declarative pipelines (Delta Live Tables), for the same testability reason.
- Hourly data, other parameters than rain/temperature/wind, other departments.
- Machine learning or forecasting.
- Deploying from CI (the Free Edition has no account-level API; see Constraints).

## Anti-goals

- Logic that only exists in notebooks.
- Tests that call data.gouv.fr or Databricks.
- A pipeline that duplicates rows when it runs again.

## Constraints

- **Databricks Free Edition** (Community Edition was retired in 2025): serverless compute
  only, Python and SQL only, one workspace, one metastore, at most 500 tables, no
  account console or account-level API. The owner creates the account and authenticates
  the Databricks CLI on their machine (`databricks auth login`); deployment
  (`databricks bundle deploy`, `bundle run`) is run from there, after the owner approves.
- Local PySpark and delta-spark versions match the Spark version of the serverless
  environment the job uses (the builder checks the current serverless environment
  version).
- Météo-France files: `;`-separated, gzip, a header row, one file per department and
  period; field meanings in the dataset's "descriptif des champs". The builder reads that
  description and the real header before writing the silver schema.
- Whether serverless compute on the Free Edition may download from the internet is
  unverified: the ingestion step must also run locally and upload to a Unity Catalog
  volume (`databricks fs cp`) if it cannot.
- Repository rules as in the other three: English code and docs, `make check` before every
  commit, one PR per feature, never call real external APIs in tests.
- The portfolio's root `README.md` and `CLAUDE.md` list the new repository; the French
  documentation gets a page `/projets/antilles-climate-lakehouse`, a card on `/projets`
  and glossary terms (medallion, Delta, Unity Catalog).

## Approaches

| Approach | Trade-off |
| --- | --- |
| **PySpark package + Databricks Asset Bundle** (chosen) | Tested locally and in CI, deployed as code as a serverless job; deployment runs from the owner's machine, not CI. |
| Lakeflow declarative pipelines | Modern, built-in quality expectations; runs only on Databricks, so barely testable in CI. |
| Notebooks exported to the repo | Fast and visual; untestable, and common. |

The owner picked the first. Exploration notebooks may sit on top, calling the package, for
show.

## Design

### Repository layout

```
antilles-climate-lakehouse/
├── src/antilles_lakehouse/
│   ├── sources.py     data.gouv.fr API: list and download the 971/972 RR-T-Vent files
│   ├── bronze.py      raw rows → bronze table (append, file-level idempotence)
│   ├── silver.py      typing, quality, nulls → MERGE into silver; station dimension
│   ├── gold.py        monthly climate, anomalies vs 1991–2020, extremes, records
│   ├── tables.py      table names per environment (local paths vs Unity Catalog)
│   └── jobs.py        entry points for the job tasks (ingest, bronze, silver, gold)
├── resources/climate_job.yml   the job: four tasks in order, serverless, monthly schedule
├── databricks.yml              the bundle (targets: dev)
├── tests/                      local Spark session fixture, small fixture files in the
│                               real Météo-France format, HTTP mocked
└── docs/images/                charts and screenshots
```

### Names

Unity Catalog: catalog `antilles`, schema `climate`, tables `bronze_daily`,
`silver_daily`, `silver_stations`, `gold_monthly`, `gold_anomalies`, `gold_extremes`,
volume `landing` for the downloaded files. Locally: a Spark warehouse directory and a local
landing folder, same table names. (The builder checks that the Free Edition lets a user
create a catalog; if not, the workspace's default catalog is used and the README says so.)

### Republished files (my call)

Météo-France rewrites its files in place (new `last_modified`, same URL), even the large
"previous" history, and the periods roll over each year (`latest-2025-2026` →
`latest-2026-2027`, `previous-1950-2024` → `previous-1950-2025`).

- A file's identity is (resource id, checksum). The same checksum again is skipped: that is
  the "loaded twice, no duplicate" test.
- Bronze holds **the current copy of each resource**: a changed resource replaces its own
  rows (`replaceWhere` on the resource id) instead of appending a second full copy; Delta's
  history keeps the earlier copies (time travel), so nothing is lost and bronze does not
  grow by millions of rows a month.
- Rows of a resource that disappeared from the API (an old "latest" period) stay in bronze
  until its rows are covered by the new "previous" file.
- Silver keeps, per (station, date), the row from the resource with the most recent
  `last_modified`, then `MERGE`s: inserted if new, updated only if a value changed, so a
  correction published by Météo-France flows through and an unchanged republication
  writes nothing. This is what makes the monthly schedule useful.

### Territories and a changing station network (my calls)

- The 971 files cover Guadeloupe and also Saint-Barthélemy and Saint-Martin (over 200 km
  north, a different climate). `silver_stations.territory` is one of `Guadeloupe`
  (the archipelago: Basse-Terre, Grande-Terre, Marie-Galante, Les Saintes, La Désirade),
  `Saint-Barthelemy`, `Saint-Martin`, `Martinique`, derived from the station's commune
  (INSEE code in the station id: 97123 Saint-Barthélemy, 97127 Saint-Martin). The story
  and charts focus on Guadeloupe and Martinique; the two northern islands stay in the
  tables.
- Stations open and close over the decades, so summing station-days would show a trend
  that comes from the network, not the climate. Gold therefore:
  - computes every indicator **per station** first;
  - aggregates per territory **only over a fixed panel of long-series stations**: at least
    80 % of days present in each decade used, from 1991 to the last complete year;
    the territory value is the mean over the panel's stations;
  - stores the panel and its size next to each territory figure, and the README says
    which stations they are.
- Quality: values flagged doubtful by Météo-France stay in silver with their code but are
  excluded from gold (normals, extremes, records).

### Thresholds (gold)

- Very hot day: daily maximum temperature ≥ 32 °C.
- Heavy-rain day: daily rainfall ≥ 50 mm.
- Normal: mean over 1991–2020 per station and calendar month, only where at least 20 of
  the 30 years have data for that month.

### Tests

- Unit (local Spark): each layer on fixture files, including a file loaded twice (no
  duplicate), a republished file with one corrected value (bronze replaces the resource's
  rows, silver updates that one row and no other), a period rollover (the same day in an
  old "latest" and a new "previous" file: one silver row, from the newer file), a station
  that opens mid-series (excluded from the territory panel, kept per station), a sentinel or
  out-of-range value (nulled), a station with too few years (no normal), time travel
  (the silver table's previous version still readable).
- `sources.py` with mocked HTTP: picks the right resources for 971/972 from an API response
  fixture.
- CI: Java + PySpark on GitHub Actions; `databricks bundle validate` if it can run without
  a workspace, otherwise left out and said so.

### Docs

README (story, architecture, layers, Delta features, how to deploy to the Free Edition,
charts and screenshots), `docs/ROADMAP.md` for the repo's own next steps, `CLAUDE.md`,
root portfolio README/CLAUDE.md, French page + card + glossary in `antilles-data-pipeline`.

## Left to the builder

- Spark / delta-spark / databricks-connect versions (matching serverless).
- The silver schema, from the real header and field description.
- Chart tooling for the README images (from the gold tables, reproducible by a script).
- Whether the job downloads on Databricks or the owner uploads (per the egress check).
