#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "${repo_root}/outputs"

project="${DBT_BQ_DEV_PROJECT:-wolt-assignment-489610}"
dataset="${DBT_BQ_DEV_DATASET:-analytics_dev}"
export CLOUDSDK_CONFIG="${repo_root}/.gcloud"

bq --project_id="${project}" query \
  --nouse_legacy_sql \
  --format=csv \
  --max_rows=1000000000 \
  "select * from \`${project}.${dataset}.fct_order_item\` order by order_ts_utc, order_item_sk" \
  > "${repo_root}/outputs/task1_order_item_enriched.csv"

echo "Wrote outputs/task1_order_item_enriched.csv"
