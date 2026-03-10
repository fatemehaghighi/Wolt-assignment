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
  "with latest_snapshot as (
      select max(snapshot_date) as snapshot_date
      from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_rpt.rpt_category_daily\`
    )
    select
      snapshot_date,
      date_day,
      item_category,
      orders,
      customers,
      customers_whose_first_order_included_category,
      customers_with_repeat_orders_including_category,
      units_sold,
      promo_units_sold,
      order_item_rows_revenue_eur,
      order_item_rows_discount_eur,
      avg_selling_price_eur,
      avg_order_units_for_orders_with_category,
      avg_delivery_distance_meters
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_rpt.rpt_category_daily\`
    where snapshot_date = (select snapshot_date from latest_snapshot)
    order by date_day, item_category" \
  > "${repo_root}/outputs/task2_category_growth_metrics.csv"

bq --project_id="${DBT_BQ_DEV_PROJECT}" query \
  --nouse_legacy_sql \
  --format=csv \
  --max_rows=1000000000 \
  "with latest_snapshot as (
      select max(snapshot_date) as snapshot_date
      from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_rpt.rpt_customer_promo_behavior\`
    )
    select
      snapshot_date,
      customer_sk,
      customer_key,
      first_order_ts_utc,
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
    where snapshot_date = (select snapshot_date from latest_snapshot)
    order by customer_sk" \
  > "${repo_root}/outputs/task2_customer_promo_behavior.csv"

bq --project_id="${DBT_BQ_DEV_PROJECT}" query \
  --nouse_legacy_sql \
  --format=csv \
  --max_rows=1000000000 \
  "with latest_snapshot as (
      select max(snapshot_date) as snapshot_date
      from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_rpt.rpt_item_pair_affinity\`
    )
    select
      snapshot_date,
      period_month,
      item_key_sk_1,
      item_key_sk_2,
      item_name_preferred_1,
      item_name_preferred_2,
      item_category_1,
      item_category_2,
      orders_together,
      support,
      confidence_1_to_2,
      confidence_2_to_1,
      lift
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_rpt.rpt_item_pair_affinity\`
    where snapshot_date = (select snapshot_date from latest_snapshot)
    order by period_month, item_key_sk_1, item_key_sk_2" \
  > "${repo_root}/outputs/task2_item_pair_affinity.csv"

echo "Wrote outputs/task2_category_growth_metrics.csv"
echo "Wrote outputs/task2_customer_promo_behavior.csv"
echo "Wrote outputs/task2_item_pair_affinity.csv"
