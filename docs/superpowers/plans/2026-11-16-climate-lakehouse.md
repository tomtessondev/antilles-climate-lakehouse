# Antilles Climate Lakehouse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new repository that turns Météo-France's daily climate series for Guadeloupe and Martinique into bronze, silver and gold Delta tables, tested locally and deployed to Databricks Free Edition as a serverless job.

**Architecture:** A Python package of PySpark functions (one module per layer) that take table names and a `SparkSession`, so the same code runs on a local Spark with delta-spark (tests, CI, `make run-local`) and on Databricks (job tasks calling `jobs.py` entry points, deployed by an Asset Bundle). Files are listed through the data.gouv.fr API and landed as-is; everything after landing is Spark.

**Tech Stack:** Python 3.12, PySpark + delta-spark (versions of the Databricks serverless environment), httpx + respx, pytest, ruff, mypy `--strict`, Databricks CLI / Asset Bundles, matplotlib (README charts).

**Spec:** `docs/superpowers/specs/2026-11-16-climate-lakehouse-design.md`

## Global Constraints

- Repository `antilles-climate-lakehouse` in `~/Desktop/Freelance/antilles-data-portfolio/`, package `antilles_lakehouse` (src layout, hatchling), English code/docs/commits, `make check` (ruff, ruff format --check, mypy --strict on `src`, pytest) before every commit; HTTP always mocked in tests.
- Dataset id `6569b51ae64326786e4e8e1a`; resources whose title matches `^QUOT_departement_(971|972)_periode_(\d{4}-\d{4})_RR-T-Vent$` (the `avant-1949` files are left out). The API's `checksum` is null: identity is (resource id, sha1 of the downloaded bytes).
- Source columns used (`;`-separated, gzip, header row): `NUM_POSTE, NOM_USUEL, LAT, LON, ALTI, AAAAMMJJ, RR, QRR, TN, QTN, TX, QTX, TM, QTM, FFM, QFFM, FXI, QFXI`. Quality code 2 = doubtful; empty = missing.
- Unity Catalog `antilles.climate`; tables `bronze_daily`, `silver_daily`, `silver_stations`, `gold_monthly`, `gold_anomalies`, `gold_extremes`, `gold_records`; volume `landing`.
- Territories: station id prefix `97123` → `Saint-Barthelemy`, `97127` → `Saint-Martin`, other `971` → `Guadeloupe`, `972` → `Martinique`.
- Physical bounds (outside → null in silver): temperatures −10…50 °C, rain 0…1500 mm, wind 0…100 m/s.
- Very hot day `tx_c >= 32`; heavy-rain day `rr_mm >= 50`; normal = 1991–2020 mean per station and calendar month, where at least 20 years have that month complete (≥ 80 % of its days with a non-doubtful value).
- Territory panel per indicator: stations with ≥ 80 % of days present (non-null, non-doubtful) in each of 1991–2000, 2001–2010, 2011–2020 and 2021–last complete year.
- Deployment (`databricks bundle deploy/run`), GitHub repository creation and pushes: only after the owner says go.

## Review Focus

- A file republished with the same bytes must write nothing anywhere — Task 3 `test_same_file_twice_writes_nothing` checks the Delta version numbers of bronze and silver do not move.
- A day present in an old "latest" file and the new "previous" file must give one silver row, from the newer file — Task 4 `test_period_rollover_keeps_one_row_from_the_newer_file`.
- Doubtful values must not reach normals, extremes or records — Task 5 `test_doubtful_values_stay_out_of_gold`.
- A station opening mid-series must not enter the territory panel (it would fake a trend) — Task 5 `test_station_opening_late_is_not_in_the_panel`.
- A month with too few days must not produce a normal or an anomaly — Task 5 `test_incomplete_months_make_no_normal`.

---

### Task 1: Repository skeleton and a local Delta Spark

**Files:** create `pyproject.toml`, `Makefile`, `.gitignore`, `README.md` (stub), `CLAUDE.md`, `.github/workflows/ci.yml`, `src/antilles_lakehouse/__init__.py`, `src/antilles_lakehouse/spark.py`, `tests/conftest.py`, `tests/test_spark.py`

**Interfaces — produces:**
- `antilles_lakehouse.spark.local_session(warehouse: Path) -> SparkSession` — delta-spark configured (`configure_spark_with_delta_pip`), UTC session time zone, warehouse dir.
- fixture `spark` (session-scoped, tmp warehouse) and `tmp_warehouse`.

- [ ] **Step 1:** check the current Databricks serverless environment version's Spark/Python versions (docs) and pin `pyspark` and `delta-spark` to match (ledger the versions). `git init -b main` in the new folder.
- [ ] **Step 2: failing test** `tests/test_spark.py::test_delta_round_trip_and_time_travel`: write a 2-row DataFrame as Delta table `t`, overwrite with 1 row, assert `spark.read.option("versionAsOf", 0).table("t").count() == 2` and current count 1.
- [ ] **Step 3:** `pytest` → FAIL (no package). Implement `spark.py`, `pyproject.toml` (deps; dev: pytest, pytest-cov, ruff, mypy, respx), Makefile (`install`, `check`, `run-local`), CI (setup-python 3.12, setup-java 17, `make check`), `.gitignore` (`.venv`, `spark-warehouse`, `data/`, `metastore_db`, `derby.log`), CLAUDE.md in the style of the sibling repos (purpose, stack, commands, rules).
- [ ] **Step 4:** `.venv/bin/python -m pip install -e ".[dev]"`, `make check` → pass.
- [ ] **Step 5:** commit "Start the climate lakehouse: package, local Delta Spark, CI".

### Task 2: Listing and landing the Météo-France files

**Files:** create `src/antilles_lakehouse/sources.py`, `tests/test_sources.py`, `tests/fixtures/dataset.json` (trimmed real API response: the 971/972 resources plus a few others)

**Interfaces — produces:**
- `@dataclass(frozen=True) class Resource: id: str; title: str; url: str; last_modified: datetime; department: str; period: str`
- `def climate_resources(dataset: dict[str, Any]) -> list[Resource]` (title filter of Global Constraints)
- `@dataclass(frozen=True) class LandedFile: resource: Resource; path: Path; sha1: str`
- `def land(resources: list[Resource], landing: Path, client: httpx.Client) -> list[LandedFile]` — downloads each to `landing/<resource id>.csv.gz`, computes sha1 while streaming.
- `def fetch_dataset(client: httpx.Client) -> dict[str, Any]` — `GET https://www.data.gouv.fr/api/1/datasets/6569b51ae64326786e4e8e1a/`

- [ ] **Step 1: failing tests**: `test_only_daily_rain_temperature_wind_files_of_971_and_972` (fixture → exactly the four `1950-2024`/`2025-2026` RR-T-Vent titles, departments `{"971","972"}`); `test_land_streams_and_fingerprints_each_file` (respx serves bytes; file written; `sha1` equals `hashlib.sha1(bytes).hexdigest()`).
- [ ] **Step 2:** run → FAIL. **Step 3:** implement. **Step 4:** `make check` → pass. **Step 5:** commit "List and land the Météo-France daily files".

### Task 3: Bronze

**Files:** create `src/antilles_lakehouse/bronze.py`, `tests/test_bronze.py`, `tests/fixtures/` small `.csv.gz` files in the real format (built by a test helper from rows, real header)

**Interfaces — produces:** `def load_bronze(spark: SparkSession, files: list[LandedFile], table: str) -> list[str]` — returns the resource ids actually (re)loaded. Columns: every source column as string + `resource_id`, `resource_title`, `last_modified` (timestamp), `sha1`, `ingested_at`. A resource whose current sha1 equals the stored one is skipped; a changed one replaces its rows (`replaceWhere resource_id = …`); a new one is appended.

- [ ] **Step 1: failing tests**: `test_first_load_keeps_every_row_as_text`; `test_same_file_twice_writes_nothing` (Delta history length of bronze unchanged after the second call, return `[]`); `test_republished_file_replaces_its_rows_and_history_keeps_the_old` (count unchanged, new value present, `versionAsOf` previous shows the old value).
- [ ] **Step 2–4:** RED, implement, `make check`. **Step 5:** commit "Load the raw rows into bronze, one current copy per file".

### Task 4: Silver

**Files:** create `src/antilles_lakehouse/silver.py`, `tests/test_silver.py`

**Interfaces — produces:**
- `def build_silver(spark: SparkSession, bronze: str, daily: str, stations: str) -> None`
- `silver_daily` columns: `station_id string, date date, rr_mm, tn_c, tx_c, tm_c, ffm_ms, fxi_ms double, q_rr, q_tn, q_tx, q_tm, q_ffm, q_fxi int, source_resource string, source_last_modified timestamp`; key (station_id, date); liquid clustering or partitioning left to the builder per Delta version.
- `silver_stations`: `station_id, name, lat, lon, altitude_m, territory, first_date, last_date`.

Rules: per (station_id, date) keep the row of the most recent `last_modified`; values outside the physical bounds → null (quality code kept); `MERGE` updates a row only when any value or quality code differs, inserts new keys.

- [ ] **Step 1: failing tests**: `test_types_and_bounds` (`"60.5"` °C → null, `"0.8"` mm → 0.8); `test_period_rollover_keeps_one_row_from_the_newer_file`; `test_correction_updates_only_that_row` (Delta `operationMetrics` of the last MERGE: `numTargetRowsUpdated == 1`, inserted 0); `test_unchanged_republication_updates_nothing` (updated 0); `test_territories` (97123…, 97127…, 97101…, 97209… → the four names); `test_previous_version_is_still_readable` (time travel on `silver_daily`).
- [ ] **Step 2–4:** RED, implement, `make check`. **Step 5:** commit "Type, clean and merge the daily observations into silver".

### Task 5: Gold

**Files:** create `src/antilles_lakehouse/gold.py`, `tests/test_gold.py` (synthetic silver frames over 1991–2025 built in the test, small: 3 stations, a few days per month is not enough — use full months for the stations that must qualify)

**Interfaces — produces:** `def build_gold(spark: SparkSession, daily: str, stations: str, prefix: str) -> None` writing (overwrite) `gold_monthly(station_id, territory, year, month, days, tx_mean, tn_mean, rr_sum, very_hot_days, heavy_rain_days)`, `gold_anomalies(station_id, territory, year, month, tx_mean, tx_normal, tx_anomaly, rr_sum, rr_normal, rr_anomaly)`, `gold_extremes(territory, year, indicator, panel_size, mean_per_station)` with indicator in `{"very_hot_days", "heavy_rain_days"}`, `gold_records(station_id, territory, indicator, value, date)` (`max_tx_c`, `max_rr_mm`, `max_fxi_ms`). Doubtful (code 2) values excluded everywhere in gold. `last complete year` = the last year with all 12 months in silver for the panel stations.

- [ ] **Step 1: failing tests**: `test_counts_very_hot_and_heavy_rain_days` (tx 32.0 counts, 31.9 does not; rr 50 counts); `test_doubtful_values_stay_out_of_gold` (a 40 °C with q_tx 2 → not a record, not a hot day); `test_incomplete_months_make_no_normal`; `test_anomaly_is_month_mean_minus_normal`; `test_station_opening_late_is_not_in_the_panel` (panel_size counts only the long-series station; its value is the territory value); `test_records_keep_the_date`.
- [ ] **Step 2–4:** RED, implement, `make check`. **Step 5:** commit "Build monthly climate, anomalies, extremes and records in gold".

### Task 6: Jobs, local run and the Databricks bundle

**Files:** create `src/antilles_lakehouse/tables.py`, `src/antilles_lakehouse/jobs.py`, `databricks.yml`, `resources/climate_job.yml`, `tests/test_jobs.py`

**Interfaces — produces:**
- `tables.py`: `@dataclass(frozen=True) class Tables: bronze, silver_daily, silver_stations, gold_prefix: str; landing: Path` with `Tables.local(root: Path)` and `Tables.unity(catalog="antilles", schema="climate")` (landing `/Volumes/antilles/climate/landing`).
- `jobs.py`: `ingest(tables)`, `bronze(tables)`, `silver(tables)`, `gold(tables)`, and `main(argv: list[str] | None = None) -> None` (`antilles-lakehouse <step> [--local DIR]`), console script `antilles-lakehouse`. `ingest` lands files and writes the landed manifest (resource id, path, sha1, last_modified) as JSON next to them so `bronze` can run as a separate task.
- Bundle: one job `climate_daily`, tasks `ingest → bronze → silver → gold` as `python_wheel_task`s on serverless (environment with the wheel), monthly schedule paused by default, target `dev` (`mode: development`), the catalog/schema/volume created by the job's first task if missing (`CREATE … IF NOT EXISTS`).
- `make run-local` = the four steps against `data/` (real download, not a test).

- [ ] **Step 1: failing test** `test_pipeline_runs_end_to_end_locally` (respx serves two fixture files; `main(["ingest","--local",tmp])` … `gold`; gold tables exist and are non-empty).
- [ ] **Step 2–4:** RED, implement, `make check`. If `databricks` CLI is available, `databricks bundle validate` (it needs a profile: skip until Task 7 if none, ledger it). **Step 5:** commit "Run the layers as job tasks, locally and as a Databricks bundle".

### Task 7: Real data, Databricks, images, docs, publication

- [ ] **Step 1:** `make run-local` on the real files; read the gold tables; sanity-check a few numbers (station counts per territory, panel sizes, a known record) and ledger them.
- [ ] **Step 2: owner action** — the owner creates a Databricks Free Edition account and runs `! databricks auth login --host <workspace url>`. Then `databricks bundle validate`; **ask the owner** before `bundle deploy` and `bundle run climate_daily`. If serverless cannot reach data.gouv.fr, land locally and `databricks fs cp` the files into the volume, run from `bronze`, and document it.
- [ ] **Step 3:** charts from the gold tables with a script `scripts/charts.py` (matplotlib, PNGs in `docs/images/`): very hot days per year per territory (panel mean, panel size shown), monthly temperature anomaly per territory; screenshots of the job run and of the catalog (browse skill with the owner's session, or the owner provides them).
- [ ] **Step 4:** README (story, architecture diagram, the three layers and their rules, republished files, territories and panel, Delta features, how to run locally and deploy, charts), `docs/ROADMAP.md` (next steps), CLAUDE.md final; `make check`; commit.
- [ ] **Step 5: ask the owner** to create the public GitHub repo (`gh repo create tomtessondev/antilles-climate-lakehouse --public`), push `main`, CI green.

### Task 8: Portfolio docs and the French page

**Files:** portfolio root `README.md`, `CLAUDE.md` (table row: folder, stack, checks); in `antilles-data-pipeline`: `dashboard/src/app/projets/antilles-climate-lakehouse/page.tsx` (structure like the other project pages: problem, architecture, layers, technologies with why and alternatives, tests, lessons, roadmap, links, charts), a card on `dashboard/src/app/projets/page.tsx`, glossary terms (médaillon, Delta Lake, Unity Catalog, Asset Bundle), `docs/ROADMAP.md` phase 4 table marks the lakehouse done.

- [ ] **Step 1:** branch `docs/climate-lakehouse` in `antilles-data-pipeline`; write the page (read `dashboard/AGENTS.md` and the sibling pages first).
- [ ] **Step 2:** `dashboard/`: lint, typecheck, test, build; root `make check`, `make transform` → pass.
- [ ] **Step 3:** commit, push and PR after the owner's go; merge only with the owner's go.
