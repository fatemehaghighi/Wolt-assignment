#!/usr/bin/env bash
set -euo pipefail

target_plist="${HOME}/Library/LaunchAgents/com.wolt.assignment.dagster.daily.plist"

launchctl disable "gui/$(id -u)/com.wolt.assignment.dagster.daily" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "${target_plist}" >/dev/null 2>&1 || true
rm -f "${target_plist}"

echo "Uninstalled launch agent: com.wolt.assignment.dagster.daily"
