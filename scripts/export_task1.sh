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
  "select *
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_core.fct_order\`
    order by order_ts_utc, order_sk" \
  > "${repo_root}/outputs/task1_orders.csv"

bq --project_id="${DBT_BQ_DEV_PROJECT}" query \
  --nouse_legacy_sql \
  --format=csv \
  --max_rows=1000000000 \
  "select *
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}_core.fct_order_item\`
    order by order_ts_utc, order_item_sk" \
  > "${repo_root}/outputs/task1_order_items.csv"

echo "Wrote outputs/task1_orders.csv"
echo "Wrote outputs/task1_order_items.csv"
