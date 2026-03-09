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
      select max(as_of_run_ts) as as_of_run_ts
      from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}.rpt_category_daily\`
    )
    select *
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}.rpt_category_daily\`
    where as_of_run_ts = (select as_of_run_ts from latest_snapshot)
    order by date_day, item_category" \
  > "${repo_root}/outputs/task2_category_growth_metrics.csv"

bq --project_id="${DBT_BQ_DEV_PROJECT}" query \
  --nouse_legacy_sql \
  --format=csv \
  --max_rows=1000000000 \
  "with latest_snapshot as (
      select max(as_of_run_ts) as as_of_run_ts
      from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}.rpt_customer_promo_behavior\`
    )
    select *
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}.rpt_customer_promo_behavior\`
    where as_of_run_ts = (select as_of_run_ts from latest_snapshot)
    order by customer_sk" \
  > "${repo_root}/outputs/task2_customer_promo_behavior.csv"

bq --project_id="${DBT_BQ_DEV_PROJECT}" query \
  --nouse_legacy_sql \
  --format=csv \
  --max_rows=1000000000 \
  "with latest_snapshot as (
      select max(as_of_run_ts) as as_of_run_ts
      from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}.rpt_item_pair_affinity\`
    )
    select *
    from \`${DBT_BQ_DEV_PROJECT}.${DBT_BQ_DEV_DATASET}.rpt_item_pair_affinity\`
    where as_of_run_ts = (select as_of_run_ts from latest_snapshot)
    order by period_month, item_key_sk_1, item_key_sk_2" \
  > "${repo_root}/outputs/task2_item_pair_affinity.csv"

echo "Wrote outputs/task2_category_growth_metrics.csv"
echo "Wrote outputs/task2_customer_promo_behavior.csv"
echo "Wrote outputs/task2_item_pair_affinity.csv"
