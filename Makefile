SHELL := /bin/bash

.PHONY: help setup-env validate-env upload-raw load-raw ingest-raw dbt-debug-dev dbt-debug-prod dbt-run-dev dbt-test-dev dbt-build-dev dbt-run-prod dbt-test-prod dbt-backfill-item-scd2-dev dbt-backfill-item-scd2-dev-full dbt-corrective-publish-dev export-task1 export-task2 build-presentation package-submission

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
	@echo "  dbt-corrective-publish-dev       Corrective rebuild + reporting publish with run metadata"
	@echo "  export-task1    Export Task 1 dataset to outputs/task1_order_item_enriched.csv"
	@echo "  export-task2    Export Task 2 dataset to outputs/task2_category_growth_metrics.csv"
	@echo "  build-presentation  Build presentation/wolt_assignment.pdf"
	@echo "  package-submission  Build exports + presentation artifacts"

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

dbt-backfill-item-scd2-dev:
	@set -a; source .env; set +a; source .venv/bin/activate; cd wolt_assignment_dbt && dbt run --profiles-dir . --target dev --select +int_wolt_item_scd2+ --vars '{"incremental_lookback_days": $(BACKFILL_DAYS), "enable_dev_sampling": false, "enable_watermark_checks": true}'

dbt-backfill-item-scd2-dev-full:
	@set -a; source .env; set +a; source .venv/bin/activate; cd wolt_assignment_dbt && dbt run --profiles-dir . --target dev --full-refresh --select +int_wolt_item_scd2+ --vars '{"enable_dev_sampling": false, "enable_watermark_checks": true}'

dbt-corrective-publish-dev:
	@set -a; source .env; set +a; source .venv/bin/activate; cd wolt_assignment_dbt && RUN_ID=$$(date -u +%Y%m%dT%H%M%SZ) && AS_OF_RUN_TS=$$(date -u +"%Y-%m-%d %H:%M:%S+00:00") && dbt run --profiles-dir . --target dev --select +int_wolt_item_scd2+ marts.reporting --vars "{\"incremental_lookback_days\": $(BACKFILL_DAYS), \"enable_dev_sampling\": false, \"enable_watermark_checks\": true, \"publish_tag\": \"$(PUBLISH_TAG)\", \"run_id\": \"$$RUN_ID\", \"as_of_run_ts\": \"$$AS_OF_RUN_TS\"}"

export-task1:
	@mkdir -p outputs
	@export CLOUDSDK_CONFIG="$$PWD/.gcloud"; PROJECT="$${DBT_BQ_DEV_PROJECT:-wolt-assignment-489610}"; DATASET="$${DBT_BQ_DEV_DATASET:-analytics_dev}"; bq --project_id=$$PROJECT query --nouse_legacy_sql --format=csv --max_rows=1000000000 "select * from \`$$PROJECT.$$DATASET.fct_order_item\` order by order_ts_utc, order_item_sk" > outputs/task1_order_item_enriched.csv

export-task2:
	@mkdir -p outputs
	@export CLOUDSDK_CONFIG="$$PWD/.gcloud"; PROJECT="$${DBT_BQ_DEV_PROJECT:-wolt-assignment-489610}"; DATASET="$${DBT_BQ_DEV_DATASET:-analytics_dev}"; bq --project_id=$$PROJECT query --nouse_legacy_sql --format=csv --max_rows=1000000000 "with latest_snapshot as (select max(as_of_run_ts) as as_of_run_ts from \`$$PROJECT.$$DATASET.rpt_category_daily\`) select * from \`$$PROJECT.$$DATASET.rpt_category_daily\` where as_of_run_ts = (select as_of_run_ts from latest_snapshot) order by date_day, item_category" > outputs/task2_category_growth_metrics.csv

build-presentation:
	@mkdir -p presentation
	@cupsfilter -m application/pdf presentation/wolt_assignment.md > presentation/wolt_assignment.pdf

package-submission: export-task1 export-task2 build-presentation
