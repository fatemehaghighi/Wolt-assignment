# Wolt Assignment Analytics Project

Production-style analytics engineering project for Wolt assignment on BigQuery + dbt.

## End-to-End Flow

`raw (GCS/BigQuery) -> staging -> intermediate -> marts (core/reporting)`

- `staging`: source-conformed models only (typing, renaming, JSON parsing)
- `intermediate`: curation, incremental watermarking, SCD2, pricing and promo logic
- `marts/core`: dimensions + facts for BI consumption
- `marts/reporting`: assignment-ready analytical outputs with run-level audit columns

## Why Two Fact Tables

- `fct_order`: order grain for order-level KPIs and customer order lifecycle metrics
- `fct_order_item`: order-item grain for promo analysis, category/item analytics, and affinity

Keeping these grains separate avoids metric duplication and simplifies joins.

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
```

## Export Final Datasets

```bash
make export-task1
make export-task2
```

Expected artifacts:

- `outputs/task1_order_item_enriched.csv`
- `outputs/task2_category_growth_metrics.csv`
- `presentation/wolt_assignment.pdf`

## More Details

- setup notes: [SETUP_LOG.md](SETUP_LOG.md)
- modeling guide: [wolt_assignment_dbt/MODELING.md](wolt_assignment_dbt/MODELING.md)
- dbt project guide: [wolt_assignment_dbt/README.md](wolt_assignment_dbt/README.md)
