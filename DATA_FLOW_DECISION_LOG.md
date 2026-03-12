# Data Flow Decision Log

## Purpose

This document is the single place to keep:
- decision flow (why each transformation exists),
- data exploration evidence (row counts/distinct keys by layer),
- monitoring and validation checks that protect future runs.

Use it with these audit models:
- `rpt_pipeline_row_flow_audit`
- `rpt_pipeline_row_flow_audit_summary`
- `rpt_item_logs_curation_audit`
- `rpt_item_logs_curation_audit_summary`
- `rpt_order_item_item_match_audit`
- `rpt_promo_item_coverage_audit`

---

## Professional Structure Used

1. Keep business reporting marts separate from audit/diagnostic marts.
2. Track row-flow and distinct-key-flow per chain (`purchase_chain`, `order_item_chain`, `item_log_chain`, `item_dim_chain`, `promo_chain`).
3. Keep curation reasons explicit (invalid time, invalid price, timestamp conflict).
4. Keep tests as contracts for expected behavior.
5. Keep one narrative doc (this file) + machine-generated audit tables for reproducibility.

---

## Item Log Chain (Raw -> Staging -> Curated -> SCD2 -> Dimensions)

### Current observed profile (latest validated snapshot)

Based on current validation artifacts:
- raw item-log rows: `648`
- distinct `item_key`: `60`
- staging item-log rows: `648`
- curated item-log rows: `471`
- SCD2 rows: `471`
- current items (one-row-per-item): `60`

Key reason for `648 -> 471`:
- curation keeps one trusted candidate per `log_item_id`,
- removes invalid-price candidates and conflicting duplicates by trust ordering.

Implemented in:
- `wolt_assignment_dbt/models/intermediate/int_wolt_item_logs_curated.sql`
- `wolt_assignment_dbt/models/intermediate/int_wolt_item_scd2.sql`
- `wolt_assignment_dbt/models/marts/core/dim_item_history.sql`
- `wolt_assignment_dbt/models/marts/core/dim_item_current.sql`

Where to inspect exact reason counts:
- `wolt_assignment_dbt/models/marts/audit/rpt_item_logs_curation_audit.sql`
- `wolt_assignment_dbt/models/marts/audit/rpt_item_logs_curation_audit_summary.sql`

Reason categories you will see:
- `excluded_all_rows_invalid_time`
- `excluded_best_candidate_invalid_price`
- `excluded_by_item_timestamp_conflict_resolution`
- `included_best_candidate_positive_price`

---

## Purchase and Order-Item Chains

### Purchase chain
- `raw.wolt_snack_store_purchase_logs -> stg_wolt_purchase_logs -> int_wolt_purchase_logs_curated -> fct_order`
- expected behavior: same purchase grain (`purchase_key`) with possible exclusion only for invalid event time.

### Order-item chain
- `stg_wolt_order_items -> int_wolt_order_items_priced -> int_wolt_order_items_promoted -> fct_order_item`
- expected behavior: row expansion in staging (basket explode), then stable row counts across priced/promoted/fact.
- diagnostic focus: unmatched item SCD mapping and reconciliation checks.

---

## Promo Chain

Now explicitly audited as:
- `raw.wolt_snack_store_promos -> stg_wolt_promos -> dim_promo`

Diagnostic focus:
- promo rows missing mapping to `dim_item_current` (`promo_rows_missing_item_dim`).

---

## What To Query (Operational Playbook)

### 1) End-to-end row flow

```sql
select
  chain_name,
  step_order,
  object_name,
  row_count,
  distinct_primary_key_count,
  row_delta_from_prev,
  status,
  diagnostic_reason
from analytics_dev_audit.rpt_pipeline_row_flow_audit
order by chain_name, step_order;
```

### 2) One-row health summary

```sql
select *
from analytics_dev_audit.rpt_pipeline_row_flow_audit_summary;
```

### 3) Item curation reasons (row-level)

```sql
select
  log_item_id,
  expected_reason,
  curation_consistency_status,
  in_curated_flag
from analytics_dev_audit.rpt_item_logs_curation_audit
order by log_item_id;
```

### 4) Item curation reasons (summary)

```sql
select *
from analytics_dev_audit.rpt_item_logs_curation_audit_summary;
```

---

## Contracts/Tests Protecting Future Data

Core item-history checks:
- `assert_item_logs_curation_consistency.sql`
- `assert_no_duplicate_item_timestamp_in_curated.sql`
- `assert_no_zero_or_negative_item_scd2_windows.sql`
- `assert_no_consecutive_identical_item_scd2_states.sql`

Cross-model consistency checks:
- `assert_all_order_items_have_item_history_match.sql` (warning-style visibility)
- `assert_promo_items_exist_in_dim_item_current.sql`
- `assert_order_reconciliation.sql`

---

## How To Refresh This Evidence

Run:

```bash
./scripts/dbt.sh build --target dev --select \
  rpt_item_logs_curation_audit \
  rpt_item_logs_curation_audit_summary \
  rpt_pipeline_row_flow_audit \
  rpt_pipeline_row_flow_audit_summary
```

Then query audit tables in BigQuery and update this document if key profile numbers change materially.
