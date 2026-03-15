# Wolt Assignment Analytics Project

Production-style analytics engineering project for Wolt assignment on BigQuery + dbt.

## End-to-End Flow

`raw (GCS/BigQuery) -> staging -> intermediate -> marts (core/reporting)`

- `staging`: source-conformed models only (typing, renaming, JSON parsing)
- `intermediate`: curation, incremental watermarking, SCD2, pricing and promo logic
- `marts/core`: dimensions + facts for BI consumption
- `marts/reporting`: assignment-ready analytical outputs (category daily/monthly are latest-state by date grain)

## BigQuery Schema Layout

Objects are separated by layer into dedicated datasets for cleaner navigation and governance:

- `${DBT_BQ_DEV_DATASET}_stg` for staging models
- `${DBT_BQ_DEV_DATASET}_int` for intermediate models
- `${DBT_BQ_DEV_DATASET}_core` for core marts (facts/dimensions)
- `${DBT_BQ_DEV_DATASET}_rpt` for business reporting marts
- `${DBT_BQ_DEV_DATASET}_audit` for audit/quality monitoring marts
- `${DBT_BQ_DEV_DATASET}_sys` for system metadata tables (`_elt_watermarks`, `_run_metadata`)

## Why Two Fact Tables

- `fct_order`: order grain for order-level KPIs and customer order lifecycle metrics
- `fct_order_item`: order-item grain for promo analysis, category/item analytics, and affinity

Keeping these grains separate avoids metric duplication and simplifies joins.
Order facts expose both UTC and Berlin-local time fields (`order_ts_utc`, `order_ts_berlin`, `order_hour_utc`, `order_hour_berlin`).

`fct_order` is intentionally modeled as a latest-state (restatable) fact table. If late/corrected source events arrive, existing `fct_order` rows can be updated by incremental merge. Category reporting models are also latest-state by their date grain; deeper replay can be added later with dedicated snapshot/change-log models.

## Promo Logic (Task 2)

Promo detection is item-level:

- promo assignment is based on item + order date within promo validity window
- `rpt_customer_promo_behavior` distinguishes promo vs non-promo items and values
- first-order promo metrics include:
  - `first_order_had_any_promo_item`
  - `first_order_had_only_promo_items`

## Repository Layout

- `data/raw/`: provided source CSVs
- `ingestion/`: upload/load utilities
- `wolt_assignment_dbt/`: dbt project
- `outputs/`: exported assignment datasets
- `presentation/`: assignment presentation artifacts

## Run Locally

```bash
source .venv/bin/activate
make setup-env
make validate-env
make ingest-raw
make dbt-build-dev
```

## Validate / Refresh

```bash
make dbt-debug-dev
./scripts/dbt.sh build --target dev
./scripts/dbt.sh build --target dev --full-refresh
make dbt-backfill-item-scd2-dev BACKFILL_DAYS=35
make dbt-backfill-orders-dev BACKFILL_DAYS=35
# Optional: disable lookback window and read full source history in incremental mode
./scripts/dbt.sh build --target dev --vars '{"enable_incremental_lookback_window": false}'
```

## Export Final Datasets

```bash
make export-task1
make export-task2
```

Underlying scripts:

- `scripts/export_task1.sh`
- `scripts/export_task2.sh`

Task 1 is exported as two grains to avoid accidental double counting of order-level metrics:
- `orders` grain: one row per order
- `order_items` grain: one row per order x item

Expected artifacts:

- `outputs/task1_orders.csv`
- `outputs/task1_order_items.csv`
- `outputs/task2_category_growth_metrics.csv`
- `outputs/task2_category_monthly_growth_metrics.csv`
- `outputs/task2_customer_promo_behavior.csv`
- `outputs/task2_item_pair_affinity.csv`
- `presentation/wolt_assignment.pdf`

## Assignment Answer Queries (Per Question)

Question-level SQL files are provided in:

- `sql/task_answers/task1/` (Q1..Q8)
- `sql/task_answers/task2/` (Q1..Q5)

These are explicit "answer queries" for the assignment prompts.
They are separate from reusable reporting marts (`rpt_*`), which are long-lived business datasets.

## Task 2 Findings Snapshot

Latest data-backed findings summary is documented in:

- `presentation/task2_findings.md`

## Dagster Orchestration

Dagster orchestration is configured in:

- `orchestration/dagster_pipeline/defs.py`
- `orchestration/workspace.yaml`

Included jobs:

- `wolt_daily_dev_pipeline_job`: validate env -> ingest raw -> dbt build (dev) -> export task outputs
- `wolt_daily_prod_pipeline_job`: dbt build (prod), triggered only after successful dev run
- `wolt_weekly_full_refresh_job`: same flow with weekly full-refresh on dev

Included schedules:

- Daily incremental: `06:00 Europe/Berlin` (enabled by default)
- Weekly full-refresh: `Sunday 07:00 Europe/Berlin` (disabled by default)

Run Dagster UI:

```bash
pip install -r requirements-orchestration.txt
make dagster-dev
```

Trigger one run from CLI:

```bash
make dagster-materialize-now
```

The daily dev-to-prod gate is handled by sensor `trigger_prod_after_dev_success`.

Run daily automatically on macOS (without opening project/IDE):

```bash
make dagster-install-daily
```

This installs a LaunchAgent that runs `wolt_daily_pipeline_job` every day at `06:00` local time.

Stop automatic daily runs:

```bash
make dagster-uninstall-daily
```

Important operational note:
- No need to keep VS Code/project window open.
- Machine must be powered on at schedule time.
- Internet is still required because pipeline runs against BigQuery/GCS.

## Optional: Open-Source BI (Lightdash)

Local Lightdash setup is included in `bi/lightdash/`.

```bash
make lightdash-up
```

Open http://localhost:8080 and follow:

- [bi/lightdash/LIGHTDASH_SETUP.md](bi/lightdash/LIGHTDASH_SETUP.md)

Auto-connect Lightdash to BigQuery dev project after first login:

```bash
make lightdash-connect
```

Stop it:

```bash
make lightdash-down
```

Performance/stability guardrails:

```bash
make lightdash-doctor
```

Safe maintenance (keeps dashboards/metadata volumes, prunes only unused Docker artifacts):

```bash
make lightdash-maintain
```

Lightdash dashboard behavior in this repo:
- `make lightdash-task1` and `make lightdash-task2` update/create chart assets and reuse existing dashboards.
- Manual tile placement/resize done in Lightdash UI is preserved by default.
- To force full dashboard reset from scripts, run with:
  - `LIGHTDASH_RECREATE_DASHBOARD=1 make lightdash-task1`
  - `LIGHTDASH_RECREATE_DASHBOARD=1 make lightdash-task2`
- Layout defaults can be controlled when creating a dashboard from script:
  - `LIGHTDASH_CHART_TILE_WIDTH` (default `24`, total grid width is `48`)
  - `LIGHTDASH_ROW_TILE_HEIGHT` (default `10`)

Task 2 dashboard coverage:
- Q1: Category monthly growth
- Q1: Star products by category
- Q2: Declining categories MoM
- Q3: Top item pair affinity
- Q4: Category consumption by Berlin-local daypart
- Q5: First-order promo acquisition

## More Details

- setup notes: [SETUP_LOG.md](SETUP_LOG.md)
- logical validation report: [LOGICAL_VALIDATION_REPORT.md](LOGICAL_VALIDATION_REPORT.md)
- data-flow decision log: [DATA_FLOW_DECISION_LOG.md](DATA_FLOW_DECISION_LOG.md)
- logical validation SQL checks: [sql/logical_validation_checks.sql](sql/logical_validation_checks.sql)
- decision flow and tradeoff log: [DECISION_FLOW.md](DECISION_FLOW.md)
- modeling guide: [wolt_assignment_dbt/MODELING.md](wolt_assignment_dbt/MODELING.md)
- dbt project guide: [wolt_assignment_dbt/README.md](wolt_assignment_dbt/README.md)
