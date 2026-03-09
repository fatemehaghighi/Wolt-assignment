# Logical Validation Report

Date: 2026-03-09  
Environment: `dev`  
BigQuery project/dataset: `wolt-assignment-489610.analytics_dev`

## Objective
Validate the pipeline deeply for logical correctness, not only test pass/fail:
- no silent data loss across layers,
- no broken joins or missing dimensional matches,
- promo and pricing logic consistency,
- SCD2 validity integrity,
- incremental observability (watermarks + run metadata).

## What Was Checked
Full check SQL is versioned in:
- [sql/logical_validation_checks.sql](sql/logical_validation_checks.sql)

Execution command:
```bash
export CLOUDSDK_CONFIG="$PWD/.gcloud"
bq --project_id=wolt-assignment-489610 query --nouse_legacy_sql < sql/logical_validation_checks.sql
```

Also executed:
```bash
make dbt-test-dev
./scripts/dbt.sh build --target dev --full-refresh
make dbt-build-dev
```

## Key Results

### 1) Layer Consistency and Integrity
- `stg_wolt_purchase_logs = 98,871`
- `int_wolt_purchase_logs_curated = 98,871`
- `fct_order = 98,871`
- `stg_wolt_order_items = 126,022`
- `fct_order_item = 126,022`
- `orphan_fct_order_item_without_order = 0`
- `fct_order_without_items = 0`
- `items_missing_scd_match_cnt = 0`

Interpretation:
- No missing rows between purchase layers.
- No order-item rows detached from parent orders.
- No un-priced/unmatched item rows in priced intermediate.

### 2) Financial Reconciliation
- `order_reconciliation_max_abs_diff = 1.4210854715202004e-14`
- `order_reconciliation_over_0_001_cnt = 0`

Interpretation:
- Item-level modeled totals reconcile with order basket totals at effectively exact precision.

### 3) Item Log Curation Behavior
- `stg_item_logs_rows = 648`
- `stg_distinct_log_item_id = 471`
- `curated_rows = 471`
- `stg_invalid_price_null_or_non_positive = 177`
- `stg_duplicate_log_item_groups = 177`
- `stg_duplicate_extra_rows = 177`

Interpretation:
- Curated drop behavior is consistent with defined trust logic and price guardrails.

Brand missingness:
- `stg_brand_name_null_rows = 30`
- `curated_brand_name_null_rows = 30`

Interpretation:
- `brand_name` null exists in source payload and is intentionally preserved.

### 4) Staging Parsing and Basket Explode
- `raw_purchase_rows = 98,871`
- `stg_purchase_rows = 98,871`
- `stg_time_order_received_null = 0`
- `stg_item_basket_json_null = 0`
- `stg_total_basket_value_null = 0`
- `stg_service_fee_null = 0`
- `stg_courier_fee_null = 0`
- `stg_delivery_distance_null = 0`
- `stg_orders_without_exploded_items = 0`

Interpretation:
- No parsing fallout in staging; all orders have exploded basket rows.

### 5) SCD2 Edge Cases
- `scd_rows = 471`
- `scd_current_rows = 60`
- `scd_zero_length_rows = 0`
- `item_keys_with_same_timestamp_multi_logs = 0`

Interpretation:
- No zero-width SCD windows and no duplicate-timestamp ambiguity for item timelines.

### 6) Promo Ambiguity and Value Bounds
- `order_items_with_multiple_promo_matches = 0`
- `promo_rows_for_unknown_items = 0`
- `customers_with_same_timestamp_multi_orders = 0`
- `max_orders_same_customer_same_ts = 0`
- `fct_order_item_discount_gt_base_rows = 0`

Interpretation:
- No promo multi-match ambiguity in current dataset.
- No discount-over-base anomalies.

### 7) Incremental Observability
Watermark rows present:
- `int_wolt_item_logs_curated`
- `int_wolt_purchase_logs_curated`

Run metadata rows present for all reporting models:
- `rpt_category_daily`
- `rpt_customer_promo_behavior`
- `rpt_item_pair_affinity`

## Problems Found and Fixed

### A) `.env` loading instability with space-containing paths
Problem:
- `source .env` in Make/scripts broke when keyfile paths contained spaces (e.g., user home path).

Fix:
- Added shared env parser:
  - [scripts/load_env.sh](scripts/load_env.sh)
- Rewired scripts/targets to use it:
  - [scripts/dbt.sh](scripts/dbt.sh)
  - [scripts/validate_env.sh](scripts/validate_env.sh)
  - [scripts/export_task1.sh](scripts/export_task1.sh)
  - [scripts/export_task2.sh](scripts/export_task2.sh)
  - [ingestion/upload_raw_to_gcs.sh](ingestion/upload_raw_to_gcs.sh)
  - [ingestion/load_raw_to_bigquery.sh](ingestion/load_raw_to_bigquery.sh)
  - [Makefile](Makefile)

Result:
- `make validate-env` and `make dbt-build-dev` run successfully without manual env workarounds.

### B) Incomplete reporting run-metadata capture
Problem:
- `_run_metadata` hooks were only configured on one reporting model.

Fix:
- Added `pre_hook=ensure_run_metadata_table()` and `post_hook=upsert_run_metadata()` to:
  - [rpt_customer_promo_behavior.sql](wolt_assignment_dbt/models/marts/reporting/rpt_customer_promo_behavior.sql)
  - [rpt_item_pair_affinity.sql](wolt_assignment_dbt/models/marts/reporting/rpt_item_pair_affinity.sql)

Result:
- `_run_metadata` now tracks all reporting marts.

## Remaining Risks (Known, Acceptable with Operational Playbook)

1. Very-late arrivals older than lookback window
- If an event arrives older than `incremental_lookback_days`, it will not be picked in normal incremental run.
- Mitigation:
  - `make dbt-backfill-item-scd2-dev BACKFILL_DAYS=<N>`
  - `make dbt-backfill-orders-dev BACKFILL_DAYS=<N>`
  - full refresh for severe incidents.

2. Source-level null business attributes (e.g., `brand_name`)
- Current logic preserves nulls to keep source fidelity.
- Mitigation:
  - Handle with fallback labels in BI/reporting layer when needed.

3. Promo overlap semantics in future data
- Current data has no multi-match promos, but future overlap could happen.
- Mitigation:
  - Existing test `assert_no_overlapping_promos_same_item` should remain active.

## Final Status
- Deep logical validation completed and documented.
- Critical logic/data consistency checks are green.
- Operational issues found during validation were fixed and re-validated.
