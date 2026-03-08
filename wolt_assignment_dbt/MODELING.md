# Modeling Layers

## Business Rules (Design Spec)
- Item logs are item attribute history over time.
- Promo window logic: `promo_start_date` is inclusive (from midnight), `promo_end_date` is exclusive (from midnight).
- Purchase logs are one row per order.
- `total_basket_value_eur` includes promo discounts but excludes service/courier fees.
- English values are preferred where available (fallback to German).
- Promo date matching is done on Berlin local order date (`Europe/Berlin`).

## Grain Decisions
- `fct_order`: one row per order.
- `fct_order_item`: one row per order x item.

## Layer Structure
- `models/sources`: raw source declarations only.
- `models/staging`: typed parsing and light cleanup.
- `models/intermediate`: reusable transformations (SCD2, pricing, promo application, order enrichment).
- `models/marts/core`: dimensions and facts with surrogate keys.
- `models/marts/reporting`: task-oriented reporting marts.

## Surrogate Key Strategy
- Surrogate keys are generated with deterministic hashes in `macros/surrogate_key.sql`.
- Core entities use surrogate PKs:
  - `order_sk`
  - `order_item_sk`
  - `customer_sk`
  - `item_key_sk`
  - `item_scd_key`
  - `promo_key`
- Business keys are retained as descriptive columns for traceability, not as warehouse PKs.

## Core Models
- `dim_item_history`: SCD2 item versions with validity windows.
- `dim_item_current`: latest item version only.
- `dim_promo`: promo definitions.
- `dim_customer`: customer lifecycle metrics.
- `dim_date`: conformed calendar.
- `fct_order`: order-level financial and behavioral metrics.
- `fct_order_item`: item-level pricing and promo metrics.

## Reporting Models
- `rpt_category_daily`
- `rpt_item_pair_affinity`
- `rpt_customer_promo_behavior`

## Data Tests
- Generic tests for keys, nullability, uniqueness, and relationships.
- Custom SQL assertions:
  - `assert_valid_promo_windows.sql`
  - `assert_no_negative_values.sql`
  - `assert_no_overlapping_item_versions.sql`
  - `assert_order_reconciliation.sql`
