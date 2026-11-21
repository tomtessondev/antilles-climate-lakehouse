.PHONY: install lint typecheck test check run-local export-local clean

# Spark needs Java 17 (the version of the Databricks serverless environment).
JAVA_HOME ?= /opt/homebrew/opt/openjdk@17
export JAVA_HOME

install:
	pip install -e ".[local,dev]"

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy

test:
	pytest

check: lint typecheck test

# The whole pipeline on a local Spark, real files from data.gouv.fr, tables under data/.
run-local:
	antilles-lakehouse ingest --local data
	antilles-lakehouse bronze --local data
	antilles-lakehouse silver --local data
	antilles-lakehouse gold --local data
	antilles-lakehouse export --local data

# Only climate.json (data/exports/), from the gold tables of a previous run-local.
export-local:
	antilles-lakehouse export --local data

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage spark-warehouse metastore_db derby.log
