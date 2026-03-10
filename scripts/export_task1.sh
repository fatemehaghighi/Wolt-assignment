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
      order_sk,
      customer_sk,
      purchase_key,
      customer_key,
      order_ts_utc,
      order_ts_berlin,
      order_date_utc,
      order_date,
      order_hour_utc,
      order_hour_berlin,
      order_day_name,
      delivery_distance_line_meters,
      total_basket_value_eur,
      wolt_service_fee_eur,
      courier_base_fee_eur,
      basket_plus_service_fee_eur,
      total_customer_paid_eur,
      total_units_in_order,
      distinct_order_item_rows_in_order,
      promo_order_item_rows_in_order,
      promo_units_in_order,
      has_any_promo_units_in_order,
      modeled_order_items_base_amount_gross_eur,
      modeled_order_items_discount_amount_gross_eur,
      modeled_order_items_final_amount_gross_eur,
      customer_order_number,
      is_first_order_for_customer
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_core.fct_order\`
    order by order_ts_utc, order_sk" \
  > "${repo_root}/outputs/task1_orders.csv"

bq --project_id="${DBT_BQ_DEV_PROJECT}" query \
  --nouse_legacy_sql \
  --format=csv \
  --max_rows=1000000000 \
  "select
      order_item_sk,
      order_sk,
      customer_sk,
      purchase_key,
      customer_key,
      item_key,
      item_key_sk,
      item_scd_sk,
      promo_sk,
      order_ts_utc,
      order_date_utc,
      order_date,
      units_in_order_item_row,
      item_unit_base_price_gross_eur,
      discount_pct_applied,
      item_unit_discount_amount_gross_eur,
      item_unit_final_price_gross_eur,
      order_item_row_base_amount_gross_eur,
      order_item_row_discount_amount_gross_eur,
      order_item_row_final_amount_gross_eur,
      is_promo_item,
      item_name_en,
      item_name_de,
      item_name_preferred,
      item_category,
      brand_name,
      vat_rate_pct
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_core.fct_order_item\`
    order by order_ts_utc, order_item_sk" \
  > "${repo_root}/outputs/task1_order_items.csv"

echo "Wrote outputs/task1_orders.csv"
echo "Wrote outputs/task1_order_items.csv"
