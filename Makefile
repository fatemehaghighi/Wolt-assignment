SHELL := /bin/bash

.PHONY: help setup-env validate-env upload-raw load-raw ingest-raw dbt-debug-dev dbt-debug-prod dbt-run-dev dbt-test-dev dbt-build-dev dbt-run-prod dbt-test-prod dbt-backfill-item-scd2-dev dbt-backfill-item-scd2-dev-full dbt-backfill-orders-dev dbt-corrective-publish-dev export-task1 export-task2 build-presentation package-submission

BACKFILL_DAYS ?= 35
PUBLISH_TAG ?= corrective

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
	@echo "  dbt-backfill-item-scd2-dev       Deep incremental backfill for item SCD2 chain on dev (default 35-day lookback)"
	@echo "  dbt-backfill-item-scd2-dev-full  Full-refresh rebuild for item SCD2 chain on dev"
	@echo "  dbt-backfill-orders-dev          Deep incremental backfill for purchase/order chain on dev"
	@echo "  dbt-corrective-publish-dev       Corrective rebuild + reporting publish with run metadata"
	@echo "  export-task1    Export Task 1 dataset to outputs/task1_order_item_enriched.csv"
	@echo "  export-task2    Export Task 2 datasets (category, promo behavior, item affinity)"
	@echo "  build-presentation  Build presentation/wolt_assignment.pdf"
	@echo "  package-submission  Build exports + presentation artifacts"

setup-env:
	@test -f .env || cp .env.example .env
	@echo "Prepared .env (edit values before running dbt)."

validate-env:
	@./scripts/validate_env.sh

upload-raw:
	@./ingestion/upload_raw_to_gcs.sh

load-raw:
	@./ingestion/load_raw_to_bigquery.sh

ingest-raw: upload-raw load-raw

dbt-debug-dev:
	@./scripts/dbt.sh debug --target dev

dbt-debug-prod:
	@./scripts/dbt.sh debug --target prod

dbt-run-dev:
	@./scripts/dbt.sh run --target dev

dbt-test-dev:
	@./scripts/dbt.sh test --target dev

dbt-build-dev:
	@./scripts/dbt.sh build --target dev

dbt-run-prod:
	@./scripts/dbt.sh run --target prod

dbt-test-prod:
	@./scripts/dbt.sh test --target prod

dbt-backfill-item-scd2-dev:
	@./scripts/dbt.sh run --target dev --select +int_wolt_item_scd2+ --vars '{"incremental_lookback_days": $(BACKFILL_DAYS), "enable_dev_sampling": false, "enable_watermark_checks": true}'

dbt-backfill-item-scd2-dev-full:
	@./scripts/dbt.sh run --target dev --full-refresh --select +int_wolt_item_scd2+ --vars '{"enable_dev_sampling": false, "enable_watermark_checks": true}'

dbt-backfill-orders-dev:
	@./scripts/dbt.sh run --target dev --select +int_wolt_purchase_logs_curated+ marts.core marts.reporting --vars '{"incremental_lookback_days": $(BACKFILL_DAYS), "enable_dev_sampling": false, "enable_watermark_checks": true}'

dbt-corrective-publish-dev:
	@RUN_ID=$$(date -u +%Y%m%dT%H%M%SZ) && AS_OF_RUN_TS=$$(date -u +"%Y-%m-%d %H:%M:%S+00:00") && ./scripts/dbt.sh run --target dev --select +int_wolt_item_scd2+ marts.reporting --vars "{\"incremental_lookback_days\": $(BACKFILL_DAYS), \"enable_dev_sampling\": false, \"enable_watermark_checks\": true, \"publish_tag\": \"$(PUBLISH_TAG)\", \"run_id\": \"$$RUN_ID\", \"as_of_run_ts\": \"$$AS_OF_RUN_TS\"}"

export-task1:
	@./scripts/export_task1.sh

export-task2:
	@./scripts/export_task2.sh

build-presentation:
	@mkdir -p presentation
	@cupsfilter -m application/pdf presentation/wolt_assignment.md > presentation/wolt_assignment.pdf

package-submission: export-task1 export-task2 build-presentation
