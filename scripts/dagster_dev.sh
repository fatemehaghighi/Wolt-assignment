#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${repo_root}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${repo_root}/.venv/bin/activate"
fi

export DAGSTER_HOME="${repo_root}/.dagster_home"
mkdir -p "${DAGSTER_HOME}"

exec dagster dev -f "${repo_root}/orchestration/dagster_pipeline/defs.py" -a defs
