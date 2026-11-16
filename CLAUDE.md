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
  - `spark.py`: local Spark session with Delta (tests, `make run-local`)
- PySpark 4.0 + delta-spark 4.0, the versions of Databricks serverless environment 4
  (Python 3.12, JDK 17). Local runs need Java 17 (`JAVA_HOME`, set by the Makefile).
- pytest (local Spark; HTTP mocked with respx), ruff, mypy --strict

## Commands

```bash
python -m venv .venv && source .venv/bin/activate
make install / check
make run-local     # whole pipeline on real files, tables under data/
```

## Rules

- Run `make check` before every commit; it must pass.
- Never call data.gouv.fr or Databricks in tests: fixtures and respx.
- Deploy to Databricks (`databricks bundle deploy/run`) only after the owner approves.
- Keep mypy strict; no `type: ignore` without a comment explaining why.
- Code, comments, commits and README in English.
- Small commits with clear messages; open a PR per feature so CI runs.
