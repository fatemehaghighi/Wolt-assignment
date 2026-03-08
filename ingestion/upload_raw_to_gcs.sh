#!/usr/bin/env bash
set -euo pipefail

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

: "${GCS_BUCKET:?GCS_BUCKET is required (e.g. gs://wolt-assignment-raw-<suffix>)}"
: "${GCS_RAW_PREFIX:=wolt_snack_store/raw}"
: "${DBT_BQ_DEV_PROJECT:?DBT_BQ_DEV_PROJECT is required}"
: "${DBT_BQ_DEV_KEYFILE:?DBT_BQ_DEV_KEYFILE is required}"

if [[ ! -d data/raw ]]; then
  echo "Missing data/raw directory."
  exit 1
fi

# Ensure bucket path format starts with gs://
if [[ "${GCS_BUCKET}" != gs://* ]]; then
  echo "GCS_BUCKET must start with gs://"
  exit 1
fi

dest="${GCS_BUCKET%/}/${GCS_RAW_PREFIX%/}"

export CLOUDSDK_CONFIG="${CLOUDSDK_CONFIG:-$PWD/.gcloud}"
mkdir -p "${CLOUDSDK_CONFIG}"
gcloud auth activate-service-account --key-file="${DBT_BQ_DEV_KEYFILE}" >/dev/null
gcloud config set project "${DBT_BQ_DEV_PROJECT}" >/dev/null

echo "Uploading CSV files from data/raw to ${dest}/ ..."
gcloud storage cp data/raw/*.csv "${dest}/"

echo "Upload completed."
