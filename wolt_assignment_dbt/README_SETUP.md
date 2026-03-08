# dbt + BigQuery Setup (Credential-Safe)

## 1) Activate environment

From repository root:

```bash
source .venv/bin/activate
```

## 2) Create local secrets and env

```bash
make setup-env
mkdir -p credentials
# put files:
# credentials/<dev-service-account>.json
# credentials/<prod-service-account>.json
```

Edit `.env` with your project, dataset, bucket, and key file paths.

## 3) Validate config

```bash
make validate-env
```

## 4) Ingest raw files (CSV -> GCS -> BigQuery raw)

```bash
make ingest-raw
```

This runs:
- `upload-raw`: `data/raw/*.csv` -> `gs://.../<prefix>/`
- `load-raw`: GCS files -> `BQ_RAW_DATASET` tables

## 5) Validate dbt connection and build staging

```bash
make dbt-debug-dev
make dbt-build-dev
```

## 6) Run production target when needed

```bash
make dbt-debug-prod
make dbt-run-prod
make dbt-test-prod
```

## 7) Weekly SCD2 safety backfill (recommended for late arrivals)

Run a deeper incremental backfill for the item SCD2 chain:

```bash
make dbt-backfill-item-scd2-dev BACKFILL_DAYS=35
```

Optional periodic hard rebuild for complete timeline healing:

```bash
make dbt-backfill-item-scd2-dev-full
```

Corrective rebuild + tagged reporting snapshot publication:

```bash
make dbt-corrective-publish-dev BACKFILL_DAYS=35 PUBLISH_TAG=late_arrival_fix
```

## Minimum IAM guidance

For assignment scope, service accounts should typically have:
- BigQuery Job User (project)
- BigQuery Data Editor (target datasets)
- BigQuery Data Viewer (source datasets)

For GCS upload/load, account also needs Storage permissions on the configured bucket.

## Important
- Never commit `.env`.
- Never commit service-account JSON files.
- This repo is intentionally configured so only templates are versioned.
