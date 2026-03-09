#!/usr/bin/env bash
set -euo pipefail

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

: "${DBT_BQ_DEV_PROJECT:?DBT_BQ_DEV_PROJECT is required}"
: "${BQ_RAW_DATASET:=raw}"
: "${GCS_BUCKET:?GCS_BUCKET is required}"
: "${GCS_RAW_PREFIX:=wolt_snack_store/raw}"
: "${DBT_BQ_DEV_KEYFILE:?DBT_BQ_DEV_KEYFILE is required}"

project="${DBT_BQ_DEV_PROJECT}"
dataset="${BQ_RAW_DATASET}"
source_prefix="${GCS_BUCKET%/}/${GCS_RAW_PREFIX%/}"

export CLOUDSDK_CONFIG="${CLOUDSDK_CONFIG:-$PWD/.gcloud}"
mkdir -p "${CLOUDSDK_CONFIG}"
gcloud auth activate-service-account --key-file="${DBT_BQ_DEV_KEYFILE}" >/dev/null
gcloud config set project "${project}" >/dev/null

# Create dataset if missing
bq --project_id="${project}" mk --dataset --location="${DBT_BQ_LOCATION:-EU}" "${project}:${dataset}" >/dev/null 2>&1 || true

echo "Loading raw.wolt_snack_store_item_logs ..."
bq --project_id="${project}" load \
  --replace \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --allow_quoted_newlines \
  "${project}:${dataset}.wolt_snack_store_item_logs" \
  "${source_prefix}/Wolt_snack_store_item_logs.csv" \
  "LOG_ITEM_ID:STRING,ITEM_KEY:STRING,TIME_LOG_CREATED_UTC:STRING,PAYLOAD:STRING"

echo "Loading raw.wolt_snack_store_promos ..."
bq --project_id="${project}" load \
  --replace \
  --source_format=CSV \
  --skip_leading_rows=1 \
  "${project}:${dataset}.wolt_snack_store_promos" \
  "${source_prefix}/Wolt_snack_store_promos.csv" \
  "PROMO_START_DATE:STRING,PROMO_END_DATE:STRING,ITEM_KEY:STRING,PROMO_TYPE:STRING,DISCOUNT_IN_PERCENTAGE:STRING"

echo "Loading raw.wolt_snack_store_purchase_logs ..."
bq --project_id="${project}" load \
  --replace \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --allow_quoted_newlines \
  "${project}:${dataset}.wolt_snack_store_purchase_logs" \
  "${source_prefix}/Wolt_snack_store_purchase_logs.csv" \
  "TIME_ORDER_RECEIVED_UTC:STRING,PURCHASE_KEY:STRING,CUSTOMER_KEY:STRING,DELIVERY_DISTANCE_LINE_METERS:STRING,WOLT_SERVICE_FEE:STRING,COURIER_BASE_FEE:STRING,TOTAL_BASKET_VALUE:STRING,ITEM_BASKET_DESCRIPTION:STRING"

echo "Raw load completed into ${project}:${dataset}."
