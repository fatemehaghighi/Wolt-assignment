# Modeling Layers

## Business Rules (Design Spec)
- Item logs are item attribute history over time.
- Promo window logic: `promo_start_date` is inclusive (from midnight), `promo_end_date` is exclusive (from midnight).
- Purchase logs are one row per order.
- `total_basket_value_eur` includes promo discounts but excludes service/courier fees.
- English values are preferred where available (fallback to German).
- Promo date matching is done on Berlin local order date (`Europe/Berlin`).

## Data Quality Assumptions (Documented)
- During raw data inspection, duplicate `log_item_id` rows were found with conflicting payload prices.
- Example observed in raw logs: `3547fd4316d3f0cba926f0aac4de2391` had same log timestamp but two payload prices (`-7.24` and `7.13`).
- Similar duplicate pattern also included `null` vs positive price variants.
- Assumption applied in curated intermediate layer:
  - keep the duplicate row with positive non-null `product_base_price_gross_eur`,
  - filter out rows with null/non-positive prices as non-business-relevant noise.
- This rule is implemented in `int_wolt_item_logs_curated.sql` and enforced by `assert_item_log_prices_positive_not_null.sql`.
- Additional observation (needs further investigation):
  - In `stg_wolt_item_logs`, some rows have `brand_name` = `null`.
  - Current count from validation checks: `30` null-brand rows out of `648` staged rows (`~4.63%`), and `30` out of `471` curated rows.
  - Action item: investigate whether null brand is expected for specific categories/vendors or indicates upstream payload quality issues.

## Investigation Notes (How Issue Was Found)
- Step 1: Filtered raw item logs by suspicious `log_item_id` values while profiling duplicates.
  - For `3547fd4316d3f0cba926f0aac4de2391`, two rows had identical `log_item_id`, `item_key`, and `time_log_created_utc`, but different `product_base_price` values (`-7.24` and `7.13`).
- Step 2: Expanded the duplicate check to all `log_item_id` values.
  - Found repeating pattern of conflicting duplicate payloads (`negative vs positive` and `null vs positive` price values).
- Step 3: Applied explicit selection logic in curated intermediate model.
  - In duplicate groups, prioritize rows with positive non-null `product_base_price_gross_eur`.
  - Then filter final curated rows to keep only positive non-null prices.

## Timestamp Parsing Issue (How Issue Was Found)
- Step 1: Spot-checked nulls in `time_item_created_in_source_utc` in staging output.
- Step 2: Traced one example back to raw payload:
  - `log_item_id = 25a4f5a95905b9c620551dd25face96c`
  - Payload value: `"time_item_created_in_source_utc": "2019-09-05 08:33:06.213"` (no trailing `Z`).
- Root cause:
  - Source field `time_item_created_in_source_utc` in payload is stored without trailing timezone (e.g., `2019-09-05 08:33:06.213`).
- Fix:
  - Parse directly with no-timezone format and explicit UTC assumption.
  - Implemented in `stg_wolt_item_logs.sql` via `safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S', ..., 'UTC')`.

## Grain Decisions
- `fct_order`: one row per order.
- `fct_order_item`: one row per order x item.
- `stg_wolt_order_items`: one row per purchase x item_key after defensive aggregation of duplicate item_key entries in the same basket.

## Layer Structure
- `models/staging`: source declarations + simple source-conformed models (renaming, type casting, JSON extraction; no business filtering/dedup).
- `models/intermediate`: reusable transformations (SCD2, pricing, promo application, order enrichment).
- `models/marts/core`: dimensions and facts exposing both surrogate keys and business keys.
- `models/marts/reporting`: task-oriented reporting marts.
- `models/marts/metrics`: semantic/metrics definitions.

## Incremental Strategy (Scale Readiness)
- `int_wolt_item_logs_curated` and `int_wolt_purchase_logs_curated` are incremental `merge` models (BigQuery).
- Keys:
  - `int_wolt_item_logs_curated`: `log_item_id`
  - `int_wolt_purchase_logs_curated`: `purchase_key`
- Incremental filter:
  - process rows newer than `watermark_ts - incremental_lookback_days`.
  - watermark is read from a lightweight metadata table (`_elt_watermarks`) instead of scanning target tables for `max(event_timestamp)`.
  - default lookback is `7` days (`dbt_project.yml` var: `incremental_lookback_days`).
  - operational toggle: set `enable_watermark_checks: false` to temporarily disable watermark logic and fall back to target-table `max(timestamp)` cutoff.
- Rationale:
  - avoids full raw-table rescans on every run,
  - avoids repeated large-table `max(timestamp)` scans as data grows,
  - still reprocesses a recent window to capture late-arriving events and corrections.
- Assumption:
  - item/purchase logs are append-oriented event streams with stable business keys.
  - if upstream emits very late historical corrections beyond lookback, increase lookback or run periodic full-refresh.
- Operational backfill paths:
  - item correction path: `make dbt-backfill-item-scd2-dev BACKFILL_DAYS=<N>`
  - purchase/order correction path: `make dbt-backfill-orders-dev BACKFILL_DAYS=<N>`

## SCD2 Correctness Under Incremental Loads
- `int_wolt_item_scd2` builds validity windows from curated item logs ordered by `time_log_created_utc` (tie-breaker: `log_item_id`).
- This is correct when the curated item-log table contains the full relevant history for each item.
- Scenario handling:
  - On-time events:
    - New events append with later timestamps; `valid_to_utc` of previous version closes as expected.
  - Late events within lookback window:
    - Included by incremental cutoff; SCD2 windows are recalculated correctly for affected item timelines.
  - Late events older than lookback window:
    - May be skipped in curated incremental load; SCD2 windows can remain stale until backfill/full-refresh.
  - Same-timestamp multiple logs for one item:
    - Deterministic ordering via `log_item_id` avoids unstable window edges.
  - Source corrections on existing `log_item_id`:
    - Curated merge updates row; SCD2 recomputes from curated state.
  - Hard deletes in source:
    - Not currently modeled as tombstones; explicit delete handling would require dedicated logic.
- Operational policy for large scale:
  - keep lookback based on observed source lateness SLA,
  - run periodic deeper backfill/full-refresh (for example weekly/monthly) to heal very-late arrivals,
  - monitor overlap/gap quality tests continuously.

## Surrogate Key Strategy
- Surrogate keys are generated with deterministic hashes in `macros/surrogate_key.sql`.
- Core entities use surrogate PKs:
  - `order_sk`
  - `order_item_sk`
  - `customer_sk`
  - `item_key_sk`
  - `item_scd_sk`
  - `promo_key`
- Business keys are retained in marts for analyst usability, debugging, exports, and source traceability.
- Core marts are SK-first for joins, but also expose natural keys:
  - order domain: `order_sk` + `purchase_key`
  - customer domain: `customer_sk` + `customer_key`
  - item domain: `item_key_sk`/`item_scd_sk` + `item_key`
- Exception for lineage/debugging:
  - `dim_item_history` keeps `log_item_id` as technical source lineage reference (not as warehouse join key).
- Rationale:
  - consistent join interface across models,
  - less coupling to source-system key volatility,
  - clearer governance boundary between raw identifiers and curated warehouse entities.

## Core Models
- `dim_item_history`: SCD2 item versions with validity windows.
- `dim_item_current`: latest item version only.
- `dim_promo`: promo definitions.
- `dim_customer`: customer lifecycle metrics.
- `dim_date`: conformed calendar.
- `fct_order`: order-level financial and behavioral metrics.
- `fct_order_item`: item-level pricing and promo metrics.

## Intermediate Layer Notes
- `stg_wolt_order_items` is the single basket JSON expansion model in staging.
- `int_wolt_order_items_priced` reuses `stg_wolt_order_items` and joins curated purchase/order attributes, instead of re-exploding basket JSON in intermediate.

## Reporting Models
- `rpt_category_daily`
- `rpt_item_pair_affinity`
- `rpt_customer_promo_behavior`
- Reporting models are published as run-level snapshots with audit columns:
  - `run_id`
  - `as_of_run_ts`
  - `as_of_run_date`
  - `publish_tag`
- Reporting model merge keys include `run_id`, so each run is reproducible and does not overwrite same-day prior runs.
- `rpt_customer_promo_behavior` is item-level for promo semantics:
  - `first_order_had_any_promo_item`
  - `first_order_had_only_promo_items`
  - promo/non-promo item counts and values.
- `rpt_item_pair_affinity` includes readable item names/categories and uses configurable threshold var:
  - `pair_affinity_min_orders_together` (default `5`).
- Pair labels are sourced from order-time fact context (`fct_order_item` by month), not current-state-only item dimension labels.
- `rpt_category_daily` semantics:
  - customer counts are category-attributed (one customer can appear in multiple categories on the same day),
  - `avg_total_items_per_order_for_orders_with_category` is average full basket size for orders containing the category.
- Run-level metadata is also written to `_run_metadata` for traceability and incident analysis.
  - current write pattern: each reporting model writes one run-level row per run (`rpt_category_daily`, `rpt_customer_promo_behavior`, `rpt_item_pair_affinity`).
- Rationale:
  - business can compare outputs across run dates,
  - discrepancies can be traced back to a specific corrective/backfill publication run.

## Data Tests
- Generic tests for keys, nullability, uniqueness, and relationships.
- Custom SQL assertions:
  - `assert_valid_promo_windows.sql`
  - `assert_no_negative_values.sql`
  - `assert_no_overlapping_item_versions.sql`
  - `assert_order_reconciliation.sql`
  - `assert_non_negative_delivery_distance.sql`
  - `assert_order_item_value_bounds.sql`

## Monetary Rounding Policy
- Promo math is modeled with exact numeric arithmetic in transformation layer.
- Intermediate/fact models do not force cent-level rounding of unit/line fields.
- Presentation/reporting layers can apply display rounding where needed.
- Reconciliation tolerance is intentionally strict (`0.001`) to stay source-conformant.

## Time Semantics
- `fct_order` exposes both UTC and Berlin-local time fields:
  - `order_ts_utc`, `order_hour_utc`
  - `order_ts_berlin`, `order_hour_berlin`
- `dim_date` is deterministic and reproducible using fixed vars:
  - `dim_date_start_date`
  - `dim_date_end_date`
