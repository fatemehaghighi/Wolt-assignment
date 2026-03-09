#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck disable=SC1091
source "${repo_root}/scripts/load_env.sh"
load_env_file "${repo_root}/.env"

: "${GCS_BUCKET:?GCS_BUCKET is required (e.g. gs://wolt-assignment-raw-<suffix>)}"
: "${GCS_RAW_PREFIX:=wolt_snack_store/raw}"
: "${DBT_BQ_DEV_PROJECT:?DBT_BQ_DEV_PROJECT is required}"
: "${DBT_BQ_DEV_KEYFILE:?DBT_BQ_DEV_KEYFILE is required}"

if [[ ! -d "${repo_root}/data/raw" ]]; then
  echo "Missing data/raw directory."
  exit 1
fi

# Ensure bucket path format starts with gs://
if [[ "${GCS_BUCKET}" != gs://* ]]; then
  echo "GCS_BUCKET must start with gs://"
  exit 1
fi

dest="${GCS_BUCKET%/}/${GCS_RAW_PREFIX%/}"

export CLOUDSDK_CONFIG="${CLOUDSDK_CONFIG:-${repo_root}/.gcloud}"
mkdir -p "${CLOUDSDK_CONFIG}"
gcloud auth activate-service-account --key-file="${DBT_BQ_DEV_KEYFILE}" >/dev/null
gcloud config set project "${DBT_BQ_DEV_PROJECT}" >/dev/null

echo "Uploading CSV files from data/raw to ${dest}/ ..."
gcloud storage cp "${repo_root}/data/raw/"*.csv "${dest}/"

echo "Upload completed."
