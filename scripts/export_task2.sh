#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "${repo_root}/outputs"

# shellcheck disable=SC1091
source "${repo_root}/scripts/load_env.sh"
load_env_file "${repo_root}/.env"

: "${DBT_BQ_DEV_PROJECT:?DBT_BQ_DEV_PROJECT is required}"
: "${DBT_BQ_DEV_DATASET:?DBT_BQ_DEV_DATASET is required}"
: "${DBT_BQ_DEV_KEYFILE:?DBT_BQ_DEV_KEYFILE is required}"

export CLOUDSDK_CONFIG="${repo_root}/.gcloud"
gcloud auth activate-service-account --key-file="${DBT_BQ_DEV_KEYFILE}" >/dev/null
gcloud config set project "${DBT_BQ_DEV_PROJECT}" >/dev/null

bq --project_id="${DBT_BQ_DEV_PROJECT}" query \
  --nouse_legacy_sql \
  --format=csv \
  --max_rows=1000000000 \
  "select
      order_month,
      metric_name,
      item_category,
      metric_value
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_rpt.rpt_category_monthly_kpi_long\`
    order by order_month, item_category, metric_name" \
  > "${repo_root}/outputs/task2_category_growth_metrics.csv"

bq --project_id="${DBT_BQ_DEV_PROJECT}" query \
  --nouse_legacy_sql \
  --format=csv \
  --max_rows=1000000000 \
  "select
      *
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_rpt.rpt_category_overall_scorecard\`
    order by revenue_eur desc" \
  > "${repo_root}/outputs/task2_category_monthly_growth_metrics.csv"

bq --project_id="${DBT_BQ_DEV_PROJECT}" query \
  --nouse_legacy_sql \
  --format=csv \
  --max_rows=1000000000 \
  "select
      customer_sk,
      customer_key,
      first_order_ts_utc,
      first_order_ts_berlin,
      first_order_had_any_promo_units,
      first_order_had_only_promo_units,
      orders_with_any_promo_units,
      orders_with_no_promo_units,
      promo_units_purchased,
      non_promo_units_purchased,
      promo_value_eur,
      non_promo_value_eur,
      promo_only_customer_flag,
      lifetime_orders
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_rpt.rpt_customer_promo_behavior\`
    order by customer_sk" \
  > "${repo_root}/outputs/task2_customer_promo_behavior.csv"

bq --project_id="${DBT_BQ_DEV_PROJECT}" query \
  --nouse_legacy_sql \
  --format=csv \
  --max_rows=1000000000 \
  "select
      product_a,
      product_b,
      pair_orders,
      product_a_orders,
      product_b_orders,
      total_orders,
      support,
      expected_support_independent,
      expected_pair_orders_independent,
      pair_orders_vs_expected_delta,
      pair_orders_vs_expected_pct,
      confidence_a_to_b,
      confidence_b_to_a,
      lift,
      category_a,
      category_b,
      pattern,
      actionability_bucket,
      actionability_rank
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_rpt.rpt_cross_sell_product_pairs\`
    order by actionability_rank asc, lift desc, pair_orders desc" \
  > "${repo_root}/outputs/task2_item_pair_affinity.csv"

echo "Wrote outputs/task2_category_growth_metrics.csv"
echo "Wrote outputs/task2_category_monthly_growth_metrics.csv"
echo "Wrote outputs/task2_customer_promo_behavior.csv"
echo "Wrote outputs/task2_item_pair_affinity.csv"
