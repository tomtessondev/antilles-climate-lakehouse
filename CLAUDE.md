# CLAUDE.md

Context for Claude Code working in this repository.

## Purpose

Portfolio project (phase 4, lakehouse): Météo-France's daily climate series for Guadeloupe
(971) and Martinique (972) as bronze, silver and gold Delta tables, the same PySpark code
tested locally and deployed to Databricks Free Edition. The owner is an FPGA / embedded
engineer moving toward freelance data engineering, so code quality, tests and clear docs
matter more than feature count. Every change should be something a recruiter can read.

## Stack

- Python 3.12+ package in `src/antilles_lakehouse` (src layout, hatchling)
  - `sources.py`: data.gouv.fr API, the 971/972 "RR-T-Vent" files, landing with sha1
  - `bronze.py` / `silver.py` / `gold.py`: the three layers, functions of a Spark session
    and table names (rules in the README: republished files, territories, station panel)
  - `export.py`: gold + station dimension as `climate.json` for the portfolio site. Its
    keys are a contract (antilles-data-pipeline `dashboard/src/lib/climate.ts`, zod): change
    both together
  - `tables.py` + `jobs.py`: table names and file folders (local or `antilles.climate.*`,
    volumes `landing` and `exports`) and the job steps
    (`antilles-lakehouse <step> [--local DIR]`)
  - `spark.py`: local Spark session with Delta (tests, `make run-local`; a Derby metastore
    for run-local so separate steps see the same tables)
- Databricks Asset Bundle (`databricks.yml`, `resources/climate_job.yml`): serverless job
  `antilles-climate`, tasks ingest → bronze → silver → gold → export, schedule paused. Workspace:
  Databricks Free Edition, CLI profile created by `databricks auth login`. PySpark is a `local` extra:
  never a dependency of the wheel (it would replace the serverless Spark).
- `scripts/charts.py`: README charts from the local gold tables.
- PySpark 4.0 + delta-spark 4.0, the versions of Databricks serverless environment 4
  (Python 3.12, JDK 17). Local runs need Java 17 (`JAVA_HOME`, set by the Makefile).
- pytest (local Spark; HTTP mocked with respx), ruff, mypy --strict
- GitHub Actions: `ci.yml` (lint, types, tests) and `publish.yml` (monthly + manual: copies
  `/Volumes/antilles/climate/exports/climate.json` with the Databricks CLI to a one-commit
  `data` branch; needs variable `DATABRICKS_HOST` and secret `DATABRICKS_TOKEN`, skips with
  a notice without them)

## Commands

```bash
python -m venv .venv && source .venv/bin/activate
make install / check
make run-local     # whole pipeline on real files, tables under data/
make export-local  # only data/exports/climate.json from the local gold tables
```

## Rules

- Run `make check` before every commit; it must pass.
- Never call data.gouv.fr or Databricks in tests: fixtures and respx.
- Deploy to Databricks (`databricks bundle deploy/run`) only after the owner approves.
- Keep mypy strict; no `type: ignore` without a comment explaining why.
- Code, comments, commits and README in English.
- Small commits with clear messages; open a PR per feature so CI runs.
