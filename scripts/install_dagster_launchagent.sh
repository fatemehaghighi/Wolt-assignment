#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
launch_agents_dir="${HOME}/Library/LaunchAgents"
target_plist="${launch_agents_dir}/com.wolt.assignment.dagster.daily.plist"
template="${repo_root}/orchestration/launchd/com.wolt.assignment.dagster.daily.plist"

mkdir -p "${launch_agents_dir}"
mkdir -p "${repo_root}/logs"

escaped_repo_root="$(printf '%s\n' "${repo_root}" | sed 's/[\/&]/\\&/g')"

sed "s|/Users/fhaghighi/Job apply/Wolt/Wolt_Assignment|${escaped_repo_root}|g" "${template}" > "${target_plist}"

launchctl bootout "gui/$(id -u)" "${target_plist}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "${target_plist}"
launchctl enable "gui/$(id -u)/com.wolt.assignment.dagster.daily"

echo "Installed and enabled: ${target_plist}"
echo "Next run: daily at 06:00 local time."
