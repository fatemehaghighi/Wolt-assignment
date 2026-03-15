# Setup Log

Date: 2026-03-08
Project: `<repo_root>`

## Goal
Install `dbt-core` locally in this repository, then verify installation location and version.

## Steps Executed

1. Checked current folder contents.

```bash
pwd && ls -la
```

Result:
- Working directory: `<repo_root>`
- Directory was empty.

2. Verified whether `dbt` was already installed.

```bash
which -a dbt
dbt --version
uv pip show dbt-core
python -m pip show dbt-core
```

Result:
- `dbt` command not found.
- `dbt-core` not found in current Python environments.

3. First install attempt using Python 3.9 virtual environment.

```bash
uv venv .venv && . .venv/bin/activate && uv pip install dbt-core dbt-bigquery
```

Result:
- Failed during dependency build (`pyarrow==21.0.0`) on Python 3.9.
- Error included missing Arrow CMake config (`ArrowConfig.cmake`).

4. Identified available local Python versions.

```bash
which -a python3.12 python3.11 python3.10 python3
python3.11 --version
```

Result:
- Python 3.11 available at `<local_python_3_11_path>`.
- Version: `Python 3.11.13`.

5. Recreated venv with Python 3.11 and reinstalled packages.

```bash
rm -rf .venv
uv venv --python <local_python_3_11_path> .venv
. .venv/bin/activate
uv pip install dbt-core dbt-bigquery
```

Result:
- Success.
- Installed:
  - `dbt-core==1.11.7`
  - `dbt-bigquery==1.11.1`

6. Verified dbt executable path and versions.

```bash
. .venv/bin/activate && which -a dbt
. .venv/bin/activate && dbt --version
```

Result:
- dbt executable:
  - `<repo_root>/.venv/bin/dbt`
- dbt versions:
  - Core: `1.11.7`
  - Plugin `bigquery`: `1.11.1`

## Current Status
`dbt-core` is installed and ready to use inside this project virtual environment.

## How to Use
From this project directory:

```bash
source .venv/bin/activate
dbt --version
```

## dbt Project Initialization (2026-03-08)

7. Initialized a dbt project (without interactive profile setup).

```bash
. .venv/bin/activate && dbt init wolt_assignment_dbt --skip-profile-setup
```

Result:
- Created project folder: `wolt_assignment_dbt/`
- Generated base files including `dbt_project.yml`, model/test/macro directories.

8. Added project-local profile template with dev/prod BigQuery outputs.

Created file:
- `wolt_assignment_dbt/profiles.yml`

Highlights:
- Profile name: `wolt_assignment_dbt`
- Default target: `dev`
- Targets: `dev`, `prod`
- Credentials and project settings sourced from environment variables.

9. Added environment variable template and quickstart instructions.

Created files:
- `.env.example`
- `wolt_assignment_dbt/README_SETUP.md`

Purpose:
- `.env.example`: placeholder variables for dev/prod BigQuery credentials and datasets.
- `README_SETUP.md`: commands for activation, env loading, connection test, and running dbt in dev/prod.

## Notes
- `dbt debug` was not executed yet because credentials in `.env.example` are placeholders.
- Next step is to fill real GCP values and run:

```bash
source .venv/bin/activate
set -a && source .env && set +a
cd wolt_assignment_dbt
dbt debug --profiles-dir . --target dev
```

## Professional Sharing Structure (2026-03-08)

10. Added root `.gitignore` for secure sharing.

Highlights:
- Ignores `.env`, `.venv`, dbt artifacts, editor/OS noise.
- Ignores credential key files and `credentials/` contents.
- Preserves `credentials/.gitkeep` and `.env.example`.

11. Added secure credential folder placeholder.

Created:
- `credentials/.gitkeep`

12. Added reviewer-safe project documentation.

Created/updated:
- `README.md`
- `wolt_assignment_dbt/README_SETUP.md`

Purpose:
- Explain stack, structure, and secure sharing policy.
- Provide reproducible quick-start for reviewers without exposing secrets.

13. Added automation for consistent local runs.

Created:
- `Makefile`
- `scripts/validate_env.sh`

Purpose:
- `make setup-env`: bootstrap `.env`
- `make validate-env`: check env vars + credential file paths
- standardized dbt commands for dev/prod targets

14. Updated env template for safer key paths.

Updated:
- `.env.example`

Change:
- Keyfile paths now use `${PWD}/credentials/...` so dbt can resolve absolute paths reliably.

## BigQuery Service Account Wiring (2026-03-08)

15. Detected and mapped provided key files.

Found in `credentials/`:
- `<dev_service_account_key.json>` (`<dev_service_account>@...`)
- `<prod_service_account_key.json>` (`<prod_service_account>@...`)

16. Updated local `.env` (not committed) with real project and key paths.

Configured:
- `DBT_BQ_DEV_PROJECT=<gcp_project_id>`
- `DBT_BQ_PROD_PROJECT=<gcp_project_id>`
- `DBT_BQ_DEV_KEYFILE=${PWD}/credentials/<dev_service_account_key.json>`
- `DBT_BQ_PROD_KEYFILE=${PWD}/credentials/<prod_service_account_key.json>`

17. Added missing Makefile target.

Added:
- `dbt-debug-prod`

18. Validated environment and connectivity.

Commands:
```bash
make validate-env
make dbt-debug-dev
make dbt-debug-prod
```

Result:
- Environment validation passed.
- `dbt debug` passed for both `dev` (`analytics_dev`) and `prod` (`analytics_prod`).

## Raw Data Ingestion and Modeling Scaffold (2026-03-08)

19. Copied assignment CSV files into repository for reproducible ingestion.

Added:
- `data/raw/Wolt_snack_store_item_logs.csv`
- `data/raw/Wolt_snack_store_promos.csv`
- `data/raw/Wolt_snack_store_purchase_logs.csv`

20. Added raw ingestion automation.

Created:
- `ingestion/upload_raw_to_gcs.sh`
- `ingestion/load_raw_to_bigquery.sh`

Design:
- Landing: local CSV -> GCS (`GCS_BUCKET` + `GCS_RAW_PREFIX`)
- Raw layer: GCS -> BigQuery dataset (`BQ_RAW_DATASET`, default `raw`)
- Uses explicit schemas and supports quoted newlines for JSON-like CSV columns.

21. Added dbt source and staging models for raw tables.

Created:
- `wolt_assignment_dbt/models/sources/src_wolt_raw.yml`
- `wolt_assignment_dbt/models/staging/stg_wolt_item_logs.sql`
- `wolt_assignment_dbt/models/staging/stg_wolt_promos.sql`
- `wolt_assignment_dbt/models/staging/stg_wolt_purchase_logs.sql`
- `wolt_assignment_dbt/models/staging/stg_wolt_schema.yml`

22. Updated project automation and docs.

Updated/created:
- `Makefile` with targets: `upload-raw`, `load-raw`, `ingest-raw`, `dbt-build-dev`
- `.env.example` and `.env` with `BQ_RAW_DATASET`, `GCS_BUCKET`, `GCS_RAW_PREFIX`
- `scripts/validate_env.sh` now validates GCS/raw variables too
- `README.md` and `wolt_assignment_dbt/README_SETUP.md` with end-to-end flow

23. Cleaned dbt starter artifacts.

- Removed default example models from `wolt_assignment_dbt/models/example/`.
- Updated `dbt_project.yml` for current staging-focused scope.

24. Validation executed.

Commands run:
```bash
make validate-env
set -a; source .env; set +a; . .venv/bin/activate && cd wolt_assignment_dbt && dbt parse --profiles-dir . --target dev
```

Result:
- Environment validation passed.
- dbt parse passed.

Note:
- `make ingest-raw` and downstream `dbt build` against warehouse were not executed yet in this step because they require GCS bucket readiness and networked BigQuery operations.

## First End-to-End Ingestion Run (2026-03-08)

25. Updated local environment for actual bucket.

- Set `GCS_BUCKET=gs://wolt-assignment-raw` in `.env`.

26. Improved ingestion scripts for reproducibility.

Updated:
- `ingestion/upload_raw_to_gcs.sh`
- `ingestion/load_raw_to_bigquery.sh`

Changes:
- use repo-local `CLOUDSDK_CONFIG=.gcloud`
- activate service account from `DBT_BQ_DEV_KEYFILE`
- set active gcloud project from env

27. Executed ingestion pipeline.

Command:
```bash
make ingest-raw
```

Result:
- Uploaded all 3 CSV files to:
  - `gs://wolt-assignment-raw/wolt_snack_store/raw/`
- Loaded BigQuery raw tables in `<gcp_project_id>:raw`:
  - `wolt_snack_store_item_logs`
  - `wolt_snack_store_promos`
  - `wolt_snack_store_purchase_logs`

28. Verified raw row counts.

Query result:
- `wolt_snack_store_item_logs`: 648
- `wolt_snack_store_promos`: 112
- `wolt_snack_store_purchase_logs`: 98871

29. Ran dbt build in dev.

Initial run surfaced duplicate `log_item_id` values in raw item logs.
Implemented deterministic deduplication in:
- `wolt_assignment_dbt/models/staging/stg_wolt_item_logs.sql`

Reran:
```bash
make dbt-build-dev
```

Result:
- Success (`PASS=14`, `ERROR=0`).

## Full dbt Layer Buildout (2026-03-08)

30. Expanded dbt architecture to professional layered design.

Added model folders:
- `models/intermediate/`
- `models/marts/core/`
- `models/marts/reporting/`
- `models/metrics/`

31. Updated staging logic to match real JSON structures.

Key changes:
- Item payload parsing now uses array-based paths:
  - `$.price_attributes[0].product_base_price`
  - localized `name` from `$.name[]` by language preference (`en`, fallback `de`)
- Added extra item attributes (`brand_name`, `number_of_units`, `weight_in_grams`)
- Purchase staging now includes `order_date`

32. Added intermediate models.

- `int_wolt_item_versions`: item SCD-like validity windows
- `int_wolt_order_items`: exploded basket lines + item snapshot + promo matching
- `int_wolt_orders_with_item_rollups`: order-level derived metrics + customer order sequence

33. Added core marts.

- `dim_date` (time spine)
- `dim_item`
- `dim_customer`
- `fct_orders`
- `fct_order_items`

34. Added reporting marts.

- `rpt_category_performance_daily`
- `rpt_customer_promo_behavior`
- `rpt_product_pair_affinity`

35. Added semantic layer definitions.

- `models/metrics/semantic_models.yml`
- Semantic models: `sm_orders`, `sm_order_items`
- Metrics: total orders, basket value, items sold, estimated revenue, estimated discount value

36. Added and updated tests.

- Intermediate, core, and reporting schema tests added.
- Time spine declaration added for semantic-layer requirements (`dim_date`).

37. Project config and source improvements.

- `dbt_project.yml` now materializes:
  - staging/intermediate as views
  - marts/core and marts/reporting as tables
- Source database now uses `{{ target.database }}` (target-aware dev/prod behavior).

38. Validation run completed.

Commands:
```bash
dbt parse --profiles-dir . --target dev
dbt build --profiles-dir . --target dev
```

Result:
- Parse successful.
- Build successful (`PASS=53`, `ERROR=0`).
