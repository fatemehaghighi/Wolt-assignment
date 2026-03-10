# Modeling Layers

## Business Rules (Design Spec)
- Item logs are item attribute history over time.
- Promo window logic: `promo_start_date` is inclusive (from midnight), `promo_end_date` is exclusive (from midnight).
- Purchase logs are one row per order.
- `total_basket_value_eur` includes promo discounts but excludes service/courier fees.
- English values are preferred where available (fallback to German).
- Promo date matching is done on Berlin local order date (`Europe/Berlin`).
- Promo-change assumption: if any promo attribute changes (for example discount/range/type), source emits a new promo record.

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

## Assumption Registry
- `item logs`:
  - Event stream is append-oriented.
  - `log_item_id` is expected to identify a logical log event; duplicate/noisy payload rows may still occur and are curated by trust rules.
  - `time_log_created_utc` is the effective ordering timestamp for item-state evolution.
- `purchase logs`:
  - One logical order per `purchase_key`.
  - No hard-delete/tombstone semantics are assumed in source.
  - `time_order_received_utc` is the event-time truth for order chronology.
- `promos`:
  - Promo validity is defined by source date windows (`promo_start_date` inclusive, `promo_end_date` exclusive).
  - Any promo attribute change is assumed to arrive as a new promo source row (no in-place overwrite of historical rows).
- `item SCD2`:
  - Tracked attributes define state change; no-op republished states should not create new versions.
  - Late arrivals beyond incremental lookback require backfill/full-refresh to guarantee timeline correction.
- `facts`:
  - `fct_order` grain is one row per order.
  - `fct_order_item` grain is one row per order-item.
  - Order/item financial reconciliation is expected within configured tolerance.
- `surrogate keys`:
  - Surrogate-key definitions are treated as contracts and should not be changed in place.
  - Identity-definition changes require versioned-key migration (`*_sk_v2`) with controlled downstream rollout.
- `time semantics`:
  - UTC timestamps are preserved for system chronology.
  - Berlin-local dates/hours are used for business interpretation (for example promo applicability and local day/hour analytics).

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

### Why Not Collapse Intermediate Steps Into One SQL
- Professional default is modular intermediate models, not one large SQL:
  - easier testing and incident isolation by step,
  - clearer lineage and ownership of each transformation,
  - safer incremental/backfill operations on specific subchains.
- Naming convention clarity:
  - `stg_*`: source-conformed preparation,
  - `int_*`: reusable business transformations,
  - `dim_*` / `fct_*`: core serving marts,
  - `rpt_*`: business-facing reporting marts,
  - `rpt_*_audit`: monitoring/quality visibility models.

## Incremental Strategy (Scale Readiness)
- `int_wolt_item_logs_curated` and `int_wolt_purchase_logs_curated` are incremental `merge` models (BigQuery).
- Keys:
  - `int_wolt_item_logs_curated`: `log_item_id`
  - `int_wolt_purchase_logs_curated`: `purchase_key`
- Incremental filter:
  - process rows newer than `watermark_ts - incremental_lookback_days`.
  - watermark is read from a lightweight metadata table (`_elt_watermarks`) instead of scanning target tables for `max(event_timestamp)`.
  - default lookback is `7` days (`dbt_project.yml` var: `incremental_lookback_days`).
  - `enable_incremental_lookback_window: true|false` controls whether lookback is applied.
    - `true` (default): cutoff = `watermark - incremental_lookback_days`.
    - `false`: cutoff = `2020-01-01` (incremental run reads full source history).
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
- SCD2 now emits versions only for true attribute changes (no-op republished states are collapsed).
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
  - Same attributes repeated on different event ids/dates (no-op republish):
    - No new SCD version is emitted unless tracked attributes changed.
    - Example observed for `item_key = 2f605027e796b8c1d897c6100903c6d7`:
      - `bcb18b1cff8a79f511e1d7afe55dbda6` (2022-12-30)
      - `1ef4ee87a85529fcc2f0c88b57277f27` (2023-12-22)
      - same attributes -> treated as one business state in SCD2.
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
  - `promo_sk`
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

### Surrogate Key Evolution Policy
- Surrogate key definitions are treated as schema contracts.
- Do not add new descriptive columns to surrogate key hash inputs unless they change true row identity.
- If a newly added column is only an attribute (not identity), keep existing surrogate key logic unchanged.
- If identity truly changes and a new input must be part of the key:
  1. introduce a versioned key (for example `promo_sk_v2`) and keep the old key during transition,
  2. backfill downstream facts/dims with mapping logic,
  3. switch consumers, then retire old key in a controlled migration.
- Avoid silent in-place key-definition changes; they can remap historical rows and break foreign-key continuity.

## Core Models
- `dim_item_history`: SCD2 item versions with validity windows.
- `dim_item_current`: latest item version only.
- `dim_promo`: promo rule definitions (lookup context, not transactional events).
- `dim_customer`: customer lifecycle metrics.
- `dim_date`: conformed calendar.
- `fct_order`: order-level financial and behavioral metrics.
- `fct_order_item`: item-level pricing and promo metrics.

### Single-Fact + Audit Monitoring Pattern
- `fct_order_item` is the single source-of-truth item fact surface (no parallel trusted version).
- Late-arrival edge case policy:
  - unmatched item dimension keys are allowed temporarily to preserve observed order-item counts,
  - dimension-match gaps are monitored via audit + warning tests (not hard publish stop).
- Monitoring assets:
  - `rpt_order_item_item_match_audit`
  - `rpt_order_item_item_match_audit_summary`
  - warning test: `assert_all_order_items_have_item_history_match.sql`
- Operational behavior:
  1. monitor unmatched rows and impacted item keys/purchases in audit reports,
  2. run item/purchase backfill healing,
  3. rerun build/tests and confirm warnings clear.

### Customer Dimension Strategy (Type 1 Assumption)
- Current assumption: `dim_customer` is a Type 1 current-state dimension, and this is sufficient for assignment analytics questions.
- Why Type 1 is acceptable here:
  - primary questions are about current customer lifecycle metrics and first/repeat behavior from order facts,
  - no explicit requirement to reconstruct "what customer dimension looked like on day X" for every historical run.
- Implication:
  - `dim_customer` values can change over time as new orders arrive (expected behavior for lifecycle attributes).

If exact historical customer-state replay is needed in future:
1. Add `dim_customer_snapshot_daily` (one row per `customer_sk` per `snapshot_date`), partitioned by `snapshot_date`.
2. Or introduce `dim_customer_scd2` if attribute-level validity windows are required.
3. Keep current `dim_customer` as current-state serving table for simple joins.

Implementation note:
- This can be implemented directly in dbt (scheduled daily run) without requiring Dagster-specific logic.
- Dagster can orchestrate and monitor run schedule/retries/dependencies, but transformation logic remains in dbt models.

### Promo Modeling Rationale (Why `dim_promo`, not fact)
- `fct_order`/`fct_order_item` represent transactional events (orders and order-items).
- Promo source rows represent discount rules with validity windows (`promo_start_date`, `promo_end_date`) that are applied to transactions.
- In this model, promo acts as reference/lookup context for event enrichment, so it is modeled as a dimension (`dim_promo`) and joined into item facts.

### Why Promo Is Not SCD2 Here
- Current promo source already provides explicit business-valid time ranges, so historical applicability is captured by date-range joins.
- Assumption: promo changes are represented as new source rows; no in-place overwrite is expected for historical records.
- SCD2 is not required unless promo attributes can change without clear new-version rows from source (or you need separate warehouse version-history semantics).
- If that future requirement appears, introduce a dedicated promo SCD2 model/versioning layer rather than changing current promo key logic in place.

## Physical Optimization (BigQuery)
- Fact tables are partitioned and clustered for scan-cost and latency control at scale:
  - `fct_order`: partition by `order_date`, cluster by `order_sk`, `customer_sk`.
  - `fct_order_item`: partition by `order_date`, cluster by `order_sk`, `item_key_sk`, `customer_sk`.
- Fact tables are also incremental `merge` models:
  - `fct_order` keyed by `order_sk`,
  - `fct_order_item` keyed by `order_item_sk`.
- Incremental cutoff for facts uses watermark table lookups (`_elt_watermarks`) with configurable lookback (`incremental_lookback_days`) to avoid full table rebuilds while still capturing late-arriving data.
- Dimension strategy is mixed by correctness/cost profile:
  - `dim_customer`: incremental `merge` keyed by `customer_sk` with watermark-driven affected-customer recomputation.
  - `dim_item_current`: incremental `merge` keyed by `item_key_sk` with watermark-driven affected-item recomputation.
  - `dim_promo`: incremental `merge` keyed by `promo_sk` (low-churn dimensional append/update pattern).
  - `dim_item_history`: intentionally kept as full table over SCD2 source for deterministic history correctness (avoids partial-window SCD drift risk).
- High-use dimensions are clustered (and where sensible, partitioned):
  - `dim_item_history`: partition by `valid_from_utc`, cluster by `item_key_sk`, `is_current`.
  - `dim_promo`: partition by `promo_start_date`, cluster by `item_key_sk`, `promo_type`.
  - `dim_item_current`, `dim_customer`: clustered on key columns.
- Why `dim_item_history` is partitioned but `dim_item_current` is not:
  - `dim_item_history` grows with each change event and is commonly queried with time predicates, so partition pruning on `valid_from_utc` reduces scans.
  - `dim_item_current` is one-row-per-item snapshot and is mostly accessed by key joins; clustering on `item_key_sk` is usually enough.
  - Partition `dim_item_current` only when current-state table becomes very large and workload repeatedly filters by time windows on a timestamp/date column.
- Reporting marts are already optimized:
  - partitioned by `snapshot_date`,
  - clustered by each mart's query grain columns.

### Clustering Policy and When to Adjust
- Using 3-4 cluster columns can be correct, but only if they match real filter/join patterns.
- Cluster columns are ordered by importance; put the most selective and most frequent predicate first.
- Good candidates:
  - high-frequency equality filters (`customer_sk`, `item_key_sk`, `order_sk`),
  - common join keys,
  - moderate-cardinality dimensions used in dashboards.
- Poor candidates:
  - columns rarely used in predicates,
  - very low-cardinality booleans as first cluster key,
  - columns only used in final `select` without filtering.

How to validate it is helping (not hurting):
1. Compare `total_bytes_processed` and `slot_ms` before/after changes for the same workload.
2. Check top expensive queries and verify they include partition filters and cluster key predicates.
3. If a cluster key is not used by real queries, remove or reorder it.

When to simplify or clear cluster keys:
- Table is small enough that clustering gives no measurable gain.
- Query patterns changed and keys are no longer used.
- Write-heavy table has high churn and cluster strategy adds maintenance overhead without scan savings.

Current tuning decisions applied:
- `fct_order`: dropped low-cardinality `has_any_promo_units_in_order` from clustering; kept join/filter-first keys (`order_sk`, `customer_sk`).
- `fct_order_item`: dropped low-cardinality `is_promo_item`; kept (`order_sk`, `item_key_sk`, `customer_sk`).
- `int_wolt_purchase_logs_curated`: reordered clustering to (`purchase_key`, `customer_key`) to prioritize join-heavy `purchase_key`.
- `int_wolt_item_logs_curated`: removed near-unique `log_item_id` from clustering; kept `item_key` for item-history access patterns.
- `dim_item_history`: removed redundant `item_key` (already represented by `item_key_sk`); kept (`item_key_sk`, `is_current`).
- `dim_customer`, `dim_item_current`: simplified to single SK clustering to reduce unnecessary write overhead.

Monitoring SQL:
- [../sql/physical_optimization_monitoring.sql](../sql/physical_optimization_monitoring.sql)

## Intermediate Layer Notes
- `stg_wolt_order_items` is the single basket JSON expansion model in staging.
- `int_wolt_order_items_priced` reuses `stg_wolt_order_items` and joins curated purchase/order attributes, instead of re-exploding basket JSON in intermediate.
- `int_wolt_order_items_priced` joins `int_wolt_item_scd2` (intermediate-to-intermediate), not `dim_item_history` (mart/core), on purpose:
  - preserves layer boundaries (`intermediate` should not depend on downstream semantic marts),
  - avoids coupling intermediate business logic to presentation/serving models,
  - keeps historical pricing join semantics tied directly to the canonical SCD2 build step.
- `int_wolt_order_items_priced` also keeps a `QUALIFY row_number()` guard after the SCD2 range join:
  - primary protection is still in SCD2 itself (non-overlap/no-invalid-window tests),
  - `QUALIFY` is retained as a defensive fallback for unexpected operational edge cases,
  - objective is to prevent duplicate order-item rows if a temporary overlap slips through during incidents/backfills.
- Performance note for this dependency:
  - if `int_wolt_item_scd2` as a view becomes expensive at scale, optimize materialization first (table/incremental + partition/cluster) instead of bypassing layer boundaries.
  - use `dim_item_history` directly in intermediate only as a temporary incident workaround, then revert once upstream materialization is tuned.

### If `int_wolt_item_scd2` View Becomes Expensive
1. Confirm with job telemetry:
   - compare `total_bytes_processed`, `slot_ms`, and runtime for `int_wolt_order_items_priced` over representative workloads.
2. Materialize SCD2 physically:
   - switch `int_wolt_item_scd2` from view to table/incremental in BigQuery,
   - partition on `valid_from_utc` (day), cluster on `item_key`/`item_key_sk`.
3. Constrain incremental recomputation:
   - recompute affected item timelines only (watermark + lookback/backfill path).
4. Validate correctness after optimization:
   - run SCD tests (`no overlap`, `no non-positive windows`, `no consecutive identical states`) and order-item reconciliation tests.
5. Re-check cost/performance delta:
   - keep the change only if measured scan and latency improvements are consistent.

## Reporting Models
- `rpt_category_daily`
- `rpt_item_pair_affinity`
- `rpt_customer_promo_behavior`
- Reporting models are published as daily snapshots (one version per `snapshot_date`).
- Reporting model merge keys include `snapshot_date` + business grain:
  - same-day reruns replace that day's snapshot rows,
  - new-day runs append a new daily snapshot.
- `rpt_customer_promo_behavior` is item-level for promo semantics:
  - `first_order_had_any_promo_units`
  - `first_order_had_only_promo_units`
  - promo/non-promo unit counts and values.
- `rpt_item_pair_affinity` includes readable item names/categories and uses configurable threshold var:
  - `pair_affinity_min_orders_together` (default `5`).
- Pair labels are sourced from order-time fact context (`fct_order_item` by month), not current-state-only item dimension labels.
- `rpt_category_daily` semantics:
  - customer counts are category-attributed (one customer can appear in multiple categories on the same day),
  - `avg_order_units_for_orders_with_category` is average full basket size for orders containing the category.
  - `avg_selling_price_eur` is weighted ASP: `sum(order_item_rows_revenue_eur) / sum(units_sold)`.
    - uses discount-included final paid amounts,
    - intentionally not `avg(item_unit_final_price_gross_eur)` to avoid unweighted row bias.
- Run metadata is written to `_run_metadata` for engineering traceability.
  - reporting tables stay business-facing and expose only `snapshot_date` as snapshot version column.
- Rationale:
  - business can compare outputs across run dates,
  - discrepancies can be traced back to a specific corrective/backfill publication run.

## Metric Semantics (Order Enrichment)
- `promo_order_item_rows_in_order` vs `promo_units_in_order` are intentionally different:
  - `promo_order_item_rows_in_order`: count of promo order-item rows (row-level incidence).
  - `promo_units_in_order`: sum of quantities for promo rows (unit-level volume).
- Example:
  - Basket lines: `A promo x3`, `B non-promo x2`, `C promo x1`
  - `promo_order_item_rows_in_order = 2` (A, C)
  - `promo_units_in_order = 4` (3 + 1)
- Why both are needed:
  - row-level explains basket composition breadth,
  - unit-level explains promo volume intensity.

- `customer_order_number` is built with:
  - `row_number() over (partition by customer_sk order by time_order_received_utc, purchase_key)`
- Purpose:
  - deterministic customer chronology,
  - first/repeat customer metrics (`is_first_order_for_customer`) and retention analysis.
- Tie case example:
  - If two orders for the same customer share identical timestamp, `purchase_key` ensures stable ordering.

### Fact Stability vs Restatement (`fct_order`)
- Event-stable fields (expected not to change unless source correction/update):
  - `purchase_key`, `customer_key`, `order_ts_utc`, `order_ts_berlin`, fees, basket/paid amounts.
- Derived lifecycle fields (can be restated under late-arrival backfills):
  - `customer_order_number`, `is_first_order_for_customer`, and dependent customer-sequence metrics.
- Why this is acceptable:
  - lifecycle position depends on complete customer history; when older missed orders arrive, ordering is recomputed.
- Reproducibility guidance:
  - reporting marts are daily-versioned by `snapshot_date`; for strict per-run replay, use `_run_metadata` + run artifacts/logs.

### Current-State Fact Assumption (`fct_order`)
- Published assumption: `fct_order` is a current-state fact (latest trusted version per `order_sk`).
- Operational meaning:
  - incremental `merge` may update existing rows when late-arriving or corrected source data is ingested.
  - business users should treat `fct_order` as the latest canonical state, not immutable history.
- Why this is sufficient for assignment scope:
  - Task 1/2 require accurate analytical answers on current data, not mandatory row-version audit history for each order change.
- If order-change history becomes a requirement:
  1. add `fct_order_snapshot_daily` (one row per `order_sk` per `snapshot_date`) for as-of replay,
  2. optionally add `fct_order_change_log` (column-level old/new diffs) for audit and root-cause workflows,
  3. keep `fct_order` as serving-layer latest-state table for simple analytics queries.

## Data Tests
- Generic tests for keys, nullability, uniqueness, and relationships.
- Custom SQL assertions:
  - `assert_valid_promo_windows.sql`
  - `assert_no_overlapping_promos_same_item.sql`
  - `assert_no_duplicate_promo_rows.sql`
  - `assert_no_negative_values.sql`
  - `assert_no_overlapping_item_versions.sql`
  - `assert_order_reconciliation.sql`
  - `assert_non_negative_delivery_distance.sql`
  - `assert_order_item_value_bounds.sql`
  - `assert_no_consecutive_identical_item_scd2_states.sql`

## Monetary Rounding Policy
- Promo math is modeled with exact numeric arithmetic in transformation layer.
- Intermediate/fact models do not force cent-level rounding of unit/line fields.
- Presentation/reporting layers can apply display rounding where needed.
- Reconciliation tolerance is intentionally strict (`0.001`) to stay source-conformant.

## Time Semantics
- `fct_order` exposes both UTC and Berlin-local time fields:
  - `order_ts_utc`, `order_hour_utc`
  - `order_ts_berlin`, `order_hour_berlin`
- `fct_order.order_date` and `fct_order_item.order_date` are intentionally aliased from `order_date_berlin`:
  - assignment scope is Berlin orders in 2023,
  - business users ask period/day analytics in local context,
  - keeping local date as canonical fact date avoids UTC-day boundary misclassification.
- Promo applicability is intentionally evaluated on `order_date_berlin` (local date), not local timestamp:
  - source promo windows are date-based (`promo_start_date`, `promo_end_date`),
  - assignment defines start as inclusive from midnight and end as exclusive from midnight,
  - date-range join on Berlin local date is the source-conformant implementation.
- Future-proofing:
  - if promos become timestamp-granular in source, switch promo matching to Berlin-local timestamp boundaries.
- `dim_date` is deterministic and reproducible using fixed vars:
  - `dim_date_start_date`
  - `dim_date_end_date`

## Decision Traceability
- For end-to-end reasoning flow (problem -> tradeoff -> decision -> control), see:
  - [../DECISION_FLOW.md](../DECISION_FLOW.md)
