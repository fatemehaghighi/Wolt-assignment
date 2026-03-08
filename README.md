# Wolt Assignment Analytics Project

This repository is structured to be shared safely for review without exposing any cloud credentials.

## Stack
- `dbt-core` + `dbt-bigquery`
- BigQuery as the shared warehouse (`raw`, `analytics_dev`, `analytics_prod`)
- GCS as landing zone for raw files
- (Next step) Dagster orchestration
- (Next step) Semantic layer definitions in dbt
- (Next step) Metabase connected to BigQuery marts

## Repository Layout
- `data/raw/`: assignment CSV source files
- `ingestion/`: scripts for GCS upload and BigQuery raw load
- `wolt_assignment_dbt/`: dbt project
- `wolt_assignment_dbt/models/staging/`: source declarations + source-conformed staging models
- `wolt_assignment_dbt/models/intermediate/`: business transformation layer
- `wolt_assignment_dbt/models/marts/`: core, reporting, and metrics marts
- `.env.example`: required environment variables (safe template)
- `credentials/.gitkeep`: placeholder for local secret files (git-ignored)
- `scripts/validate_env.sh`: checks required env vars and key files
- `Makefile`: standard local commands for reviewers
- `SETUP_LOG.md`: chronological setup log

## Credential Policy
- Never commit service-account files.
- Never commit `.env`.
- Keep credential files only in `credentials/` (already ignored by git).
- Use per-environment service accounts (separate dev and prod keys).
- Grant minimum IAM permissions needed for assignment tasks.

## Quick Start
```bash
source .venv/bin/activate
make setup-env
# edit .env and place key files in credentials/
make validate-env
make ingest-raw
make dbt-debug-dev
make dbt-build-dev
# weekly SCD2 safety backfill (deep lookback, no full-refresh)
make dbt-backfill-item-scd2-dev BACKFILL_DAYS=35
# corrective rebuild + tagged reporting publish snapshot
make dbt-corrective-publish-dev BACKFILL_DAYS=35 PUBLISH_TAG=late_arrival_fix
```

Detailed setup: [wolt_assignment_dbt/README_SETUP.md](wolt_assignment_dbt/README_SETUP.md)
Modeling overview: [wolt_assignment_dbt/MODELING.md](wolt_assignment_dbt/MODELING.md)
