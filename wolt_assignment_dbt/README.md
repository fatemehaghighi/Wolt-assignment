# wolt_assignment_dbt

dbt project for the Wolt assignment using a 3-layer warehouse model:

- `staging`: source-conformed typing, renaming, and JSON extraction only
- `intermediate`: business logic, curation, pricing, promo assignment, SCD2
- `marts`: analytics-facing core facts/dimensions and reporting marts

## Layer Structure

- `models/staging/`
- `models/intermediate/`
- `models/marts/core/`
- `models/marts/reporting/`
- `models/marts/metrics/`

## Core Modeling Decisions

- `fct_order` is the order-grain fact for order-level metrics (basket value, fees, delivery distance, order lifecycle).
- `fct_order_item` is the order-item grain fact for item/promo/category analysis.
- Both surrogate keys and business keys are exposed in marts for analyst usability and traceability.
- Item history is modeled as SCD2 (`dim_item_history`) and current snapshot (`dim_item_current`).

## Incremental + Watermark

- Curated intermediate models support incremental processing with metadata watermarks (`_elt_watermarks`).
- Backfill safety is handled by configurable lookback windows and periodic deeper backfills.
- `dim_date` is deterministic via vars (`dim_date_start_date`, `dim_date_end_date`) for reproducible outputs.

## Reporting Marts

- `rpt_customer_promo_behavior`: customer promo behavior computed from item-level promo vs non-promo composition
- `rpt_cross_sell_product_pairs`: product-pair affinity with support/confidence/lift and actionability buckets

`rpt_category_monthly` is latest-state by business grain (`order_month` + category).
Same-day reruns replace that day snapshot; new-day runs append new snapshots.
Run-level metadata (`run_id`, `as_of_run_ts`, `as_of_run_date`, `publish_tag`) is stored in system metadata tables.

## Common Commands

```bash
# parse/compile
./scripts/dbt.sh parse --target dev
./scripts/dbt.sh compile --target dev

# full project run + tests
./scripts/dbt.sh build --target dev

# full refresh
./scripts/dbt.sh build --target dev --full-refresh

# deep backfills
make dbt-backfill-item-scd2-dev BACKFILL_DAYS=35
make dbt-backfill-orders-dev BACKFILL_DAYS=35

# targeted reporting full refresh
./scripts/dbt.sh build --target dev --full-refresh --select rpt_category_monthly_kpi_long rpt_customer_promo_behavior rpt_cross_sell_product_pairs
```

## Exporting Submission Artifacts

```bash
make export-task1
make export-task2
```

Exports are written under `outputs/`.
Export scripts:
- `scripts/export_task1.sh`
- `scripts/export_task2.sh`

`export-task1` writes two files to preserve grain separation:
- `outputs/task1_orders.csv`
- `outputs/task1_order_items.csv`

`export-task2` writes four files:
- `outputs/task2_category_growth_metrics.csv`
- `outputs/task2_category_monthly_growth_metrics.csv`
- `outputs/task2_customer_promo_behavior.csv`
- `outputs/task2_item_pair_affinity.csv`
