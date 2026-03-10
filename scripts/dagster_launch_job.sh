#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${repo_root}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${repo_root}/.venv/bin/activate"
fi

export DAGSTER_HOME="${repo_root}/.dagster_home"
mkdir -p "${DAGSTER_HOME}"

cd "${repo_root}"
# Execute directly so runs don't depend on a separately healthy queued-run daemon/code-location.
dagster job execute -f orchestration/dagster_pipeline/defs.py -a defs -j wolt_daily_pipeline_job
