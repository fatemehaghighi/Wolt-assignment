SHELL := /bin/bash

.PHONY: help setup-env validate-env upload-raw load-raw ingest-raw dbt-debug-dev dbt-debug-prod dbt-run-dev dbt-test-dev dbt-build-dev dbt-run-prod dbt-test-prod

help:
	@echo "Targets:"
	@echo "  setup-env      Create .env from template if missing"
	@echo "  validate-env   Validate required environment variables and keyfile paths"
	@echo "  upload-raw     Upload local CSV files from data/raw to GCS landing path"
	@echo "  load-raw       Load CSV files from GCS into BigQuery raw dataset"
	@echo "  ingest-raw     Run upload-raw then load-raw"
	@echo "  dbt-debug-dev  Run dbt debug against dev target"
	@echo "  dbt-debug-prod Run dbt debug against prod target"
	@echo "  dbt-run-dev    Run dbt models on dev"
	@echo "  dbt-test-dev   Run dbt tests on dev"
	@echo "  dbt-build-dev  Run dbt build on dev"
	@echo "  dbt-run-prod   Run dbt models on prod"
	@echo "  dbt-test-prod  Run dbt tests on prod"

setup-env:
	@test -f .env || cp .env.example .env
	@echo "Prepared .env (edit values before running dbt)."

validate-env:
	@set -a; source .env; set +a; ./scripts/validate_env.sh

upload-raw:
	@./ingestion/upload_raw_to_gcs.sh

load-raw:
	@./ingestion/load_raw_to_bigquery.sh

ingest-raw: upload-raw load-raw

dbt-debug-dev:
	@set -a; source .env; set +a; source .venv/bin/activate; cd wolt_assignment_dbt && dbt debug --profiles-dir . --target dev

dbt-debug-prod:
	@set -a; source .env; set +a; source .venv/bin/activate; cd wolt_assignment_dbt && dbt debug --profiles-dir . --target prod

dbt-run-dev:
	@set -a; source .env; set +a; source .venv/bin/activate; cd wolt_assignment_dbt && dbt run --profiles-dir . --target dev

dbt-test-dev:
	@set -a; source .env; set +a; source .venv/bin/activate; cd wolt_assignment_dbt && dbt test --profiles-dir . --target dev

dbt-build-dev:
	@set -a; source .env; set +a; source .venv/bin/activate; cd wolt_assignment_dbt && dbt build --profiles-dir . --target dev

dbt-run-prod:
	@set -a; source .env; set +a; source .venv/bin/activate; cd wolt_assignment_dbt && dbt run --profiles-dir . --target prod

dbt-test-prod:
	@set -a; source .env; set +a; source .venv/bin/activate; cd wolt_assignment_dbt && dbt test --profiles-dir . --target prod
