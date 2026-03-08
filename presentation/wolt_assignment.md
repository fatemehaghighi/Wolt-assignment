# Wolt Assignment Summary

## 1) Problem and Input Data

- Sources: item logs, purchase logs, promos.
- Goal: reliable analytical mart for order and promo behavior.

## 2) Architecture

- Raw bucket / raw dataset
- Staging: type casting, renaming, JSON parsing
- Intermediate: curation, watermark-based incremental loads, SCD2, pricing and promo application
- Marts:
  - Core (`dim_*`, `fct_order`, `fct_order_item`)
  - Reporting (`rpt_category_daily`, `rpt_customer_promo_behavior`, `rpt_item_pair_affinity`)

## 3) Key Modeling Decisions

- Separate order-grain and order-item-grain fact tables.
- Keep both surrogate keys and business keys in marts for usability and traceability.
- Item history modeled as SCD2 with validity windows.
- Curated incrementals use metadata watermarks for scale.

## 4) Task Outputs

- Task 1 dataset: `outputs/task1_order_item_enriched.csv`
- Task 2 dataset: `outputs/task2_category_growth_metrics.csv`

## 5) Quality and Reproducibility

- Generic tests: null/unique/relationships.
- Custom tests: overlap, reconciliation, promo windows, non-negative checks.
- Reporting marts include run-level audit fields:
  - `run_id`
  - `as_of_run_ts`
  - `as_of_run_date`
  - `publish_tag`

## 6) Operational Notes

- Daily incremental runs.
- Weekly safety backfill for late-arriving events.
- Corrective publish path for sensitive late corrections.
