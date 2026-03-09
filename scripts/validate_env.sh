#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck disable=SC1091
source "${repo_root}/scripts/load_env.sh"
load_env_file "${repo_root}/.env"

required_vars=(
  DBT_BQ_LOCATION
  BQ_RAW_DATASET
  GCS_BUCKET
  GCS_RAW_PREFIX
  DBT_BQ_DEV_PROJECT
  DBT_BQ_DEV_DATASET
  DBT_BQ_DEV_KEYFILE
  DBT_BQ_PROD_PROJECT
  DBT_BQ_PROD_DATASET
  DBT_BQ_PROD_KEYFILE
)

missing=0
for var in "${required_vars[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "Missing env var: $var"
    missing=1
  fi
done

for file_var in DBT_BQ_DEV_KEYFILE DBT_BQ_PROD_KEYFILE; do
  path="${!file_var:-}"
  if [[ -n "$path" && ! -f "$path" ]]; then
    echo "Credential file not found for $file_var: $path"
    missing=1
  fi
done

if [[ -n "${GCS_BUCKET:-}" && "${GCS_BUCKET}" != gs://* ]]; then
  echo "GCS_BUCKET must start with gs://"
  missing=1
fi

if [[ "$missing" -eq 1 ]]; then
  echo "Environment validation failed."
  exit 1
fi

echo "Environment validation passed."
