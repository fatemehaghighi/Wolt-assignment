#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "${repo_root}/outputs"

if [[ -f "${repo_root}/.env" ]]; then
  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -z "${line}" || "${line}" =~ ^[[:space:]]*# ]] && continue
    line="${line#export }"
    key="${line%%=*}"
    value="${line#*=}"
    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    value="${value%$'\r'}"
    if [[ "${value}" =~ ^\".*\"$ || "${value}" =~ ^\'.*\'$ ]]; then
      value="${value:1:${#value}-2}"
    fi
    if [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
      export "${key}=${value}"
    fi
  done < "${repo_root}/.env"
fi

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
      oi.*,
      o.order_ts_berlin,
      o.order_hour_berlin,
      o.delivery_distance_line_meters,
      o.total_basket_value_eur,
      o.wolt_service_fee_eur,
      o.courier_base_fee_eur,
      o.total_customer_paid_eur,
      o.customer_order_number,
      o.is_first_order_for_customer,
      o.contains_promo_flag
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}.fct_order_item\` as oi
    inner join \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}.fct_order\` as o
      using (order_sk)
    order by o.order_ts_utc, oi.order_item_sk" \
  > "${repo_root}/outputs/task1_order_item_enriched.csv"

echo "Wrote outputs/task1_order_item_enriched.csv"
