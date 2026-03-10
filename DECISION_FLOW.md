# Decision Flow and Reasoning Log

This document captures the key thought process behind major modeling and operational decisions, including tradeoffs, risks, and controls.

## 1) Layering and Scope Boundaries

Decision:
- Keep only 3 main layers: `staging`, `intermediate`, `marts`.
- Keep `sources` definitions under staging.
- Split marts into `core`, `reporting`, and `audit` folders.

Why:
- Clear responsibilities reduce logic drift.
- Business reporting should stay separate from technical validation outputs.

Controls:
- `staging` limited to parsing/typing/renaming/JSON extraction.
- Business filters, curation, SCD, promo logic only in `intermediate`.
- Audit/reconciliation models in `marts/audit` only.

## 2) Item-Log Curation (Raw 648 -> Curated 471)

Observed issue:
- Duplicate `log_item_id` groups with conflicting payload values (for example null/negative vs positive price).

Decision:
- Curate trusted item logs in `int_wolt_item_logs_curated`.
- Keep only parseable event-time rows and positive non-null base price rows.
- Resolve duplicates deterministically.

Current curation rules:
1. Deduplicate by `log_item_id` with trust ranking.
2. Resolve same `(item_key, time_log_created_utc)` conflicts deterministically.
3. Keep positive non-null price only.

Why:
- Prevent noisy source duplicates from polluting history.
- Prevent unstable SCD windows from same-time conflicting rows.

Controls:
- `assert_item_log_prices_positive_not_null.sql`
- `assert_no_duplicate_item_timestamp_in_curated.sql`
- `rpt_item_logs_curation_audit` + consistency test

## 3) SCD2 Correctness Strategy

Observed risk:
- Same-timestamp events can create zero-length history windows.
- Very-late arrivals can leave history stale under incremental lookback.
- No-op republished item events can create artificial new versions (same attributes, new event id/date).

Decision:
- Build SCD2 from curated logs only.
- Enforce deterministic ordering and uniqueness before SCD2.
- Collapse no-op consecutive states (emit version only when tracked attributes change).
- Add explicit strict-window test.

Why:
- Historical dimensions must be stable and explainable.
- Avoid silent timeline corruption.
- Avoid inflating SCD version counts with non-business changes.

Concrete example:
- `item_key = 2f605027e796b8c1d897c6100903c6d7`
- `log_item_id = bcb18b1cff8a79f511e1d7afe55dbda6` (`2022-12-30`)
- `log_item_id = 1ef4ee87a85529fcc2f0c88b57277f27` (`2023-12-22`)
- Attributes are identical across both events; treated as one state in SCD2.

Controls:
- `assert_no_zero_or_negative_item_scd2_windows.sql`
- `assert_no_overlapping_item_versions.sql`
- `assert_no_consecutive_identical_item_scd2_states.sql`

## 4) Incremental at Scale (Watermarks)

Decision:
- Use watermark metadata table (`_elt_watermarks`) for incremental cutoffs.
- Default configurable lookback (`incremental_lookback_days`, default 7).
- Keep an emergency toggle to disable watermark logic.

Why:
- Avoid repeated full-table scans for `max(timestamp)` as data grows.
- Still capture late arrivals inside a replay window.

Controls:
- Backfill entrypoints:
  - `make dbt-backfill-item-scd2-dev BACKFILL_DAYS=<N>`
  - `make dbt-backfill-orders-dev BACKFILL_DAYS=<N>`
- Full refresh for severe incidents.

## 5) Facts/Dimensions and Performance

Decision:
- Keep two facts by grain (`fct_order`, `fct_order_item`).
- Partition/cluster high-volume facts and reporting marts.
- Use mixed dimension strategy (incremental where safe, full recompute where correctness-sensitive).
- Keep `dim_customer` as Type 1 current-state for assignment scope; add daily snapshot/SCD2 only if point-in-time replay becomes a requirement.
- Treat customer-lifecycle fields in `fct_order` (`customer_order_number`, `is_first_order_for_customer`) as restate-able derived attributes.

Why:
- Avoid double-counting and reduce query costs.
- Preserve correctness for history-heavy entities.

Controls:
- Fact relationship tests and reconciliation tests.
- Physical optimization configs in model-level `config(...)`.
- Ongoing cluster/partition effectiveness checks via:
  - `sql/physical_optimization_monitoring.sql`
- Cluster keys should be revisited when workload predicates change.
- Applied tuning pass:
  - removed low-cardinality trailing booleans from fact clustering where not primary predicates,
  - removed redundant natural-key duplicates when surrogate key already present,
  - reordered clustering to prioritize dominant join keys.
- Partitioning policy:
  - `dim_item_history` is partitioned on `valid_from_utc` due to timeline-growth and time-window access.
  - `dim_item_current` remains unpartitioned unless workload shifts to large time-window scans on current-state table.
- Layering/performance tradeoff policy:
  - `int_wolt_order_items_priced` intentionally joins `int_wolt_item_scd2` (intermediate) instead of `dim_item_history` (mart) to keep dependency direction clean.
  - If the SCD2 view becomes costly, next step is to materialize/optimize `int_wolt_item_scd2` (table/incremental + partition/cluster), not to break layer boundaries by default.
  - Temporary direct joins to mart dimensions are incident-only exceptions and should be rolled back after upstream optimization.

## 6) Promo Semantics

Decision:
- Promo logic must be item-level, not only order-level flags.
- Keep both "any promo item" and "only promo items" semantics for first-order analysis.

Why:
- Directly answers assignment question on promo-driven acquisition quality.

Controls:
- `rpt_customer_promo_behavior` includes item-level promo/non-promo counts and values.
- Assumption: any promo change is delivered as a new promo row in source data (no historical in-place overwrite).

## 7) Key Strategy (Surrogate + Business Keys)

Decision:
- Use deterministic surrogate keys for warehouse joins.
- Retain business keys in marts for traceability/debug/export usability.

Why:
- Stable joins for warehouse evolution.
- Analyst friendliness and easier incident debugging.

Controls:
- Not-null/unique/relationship tests across core marts.

## 8) Validation and Explainability

Decision:
- Keep deep logical validation as a first-class artifact (not only dbt generic tests).
- Add reason-level audit outputs for curation behavior.

Why:
- Interview and production readiness require explainable logic, not only green test counts.

Controls:
- `LOGICAL_VALIDATION_REPORT.md`
- `sql/logical_validation_checks.sql`
- `marts/audit` models + consistency tests
- Late-arrival item match monitoring:
  - keep `fct_order_item` as the single source of truth,
  - monitor unmatched item-key/version rows in `marts/audit` reports,
  - warning test (`assert_all_order_items_have_item_history_match`) for visibility without stopping pipeline.

## 9) Known Risks and Operational Playbook

Risk:
- Very-late source corrections older than lookback window.

Response:
1. Increase `incremental_lookback_days` for replay window.
2. Run targeted backfill commands for affected chain.
3. Run full refresh if incident requires complete reprocessing.

Risk:
- Source payload quality drift (for example null `brand_name`).

Response:
1. Preserve source fidelity in warehouse.
2. Track in audit docs/tests.
3. Apply BI fallback labeling when needed.
