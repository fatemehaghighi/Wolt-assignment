#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck disable=SC1091
source "${repo_root}/scripts/load_env.sh"
load_env_file "${repo_root}/.env"

# shellcheck disable=SC1091
source "${repo_root}/.venv/bin/activate"

cd "${repo_root}/wolt_assignment_dbt"

if [[ "$#" -eq 0 ]]; then
  exec dbt --help
fi

subcommand="$1"
shift
exec dbt "${subcommand}" --profiles-dir . "$@"
