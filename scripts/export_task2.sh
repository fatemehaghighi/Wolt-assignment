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
  "with latest_snapshot as (
      select max(as_of_run_ts) as as_of_run_ts
      from \`${project}.${dataset}.rpt_category_daily\`
    )
    select *
    from \`${project}.${dataset}.rpt_category_daily\`
    where as_of_run_ts = (select as_of_run_ts from latest_snapshot)
    order by date_day, item_category" \
  > "${repo_root}/outputs/task2_category_growth_metrics.csv"

echo "Wrote outputs/task2_category_growth_metrics.csv"
