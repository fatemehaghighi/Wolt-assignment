#!/usr/bin/env bash

# Load KEY=VALUE pairs from an .env-style file.
# Supports unquoted values with spaces by preserving everything after first "=".
load_env_file() {
  local env_file="${1:-.env}"

  if [[ ! -f "${env_file}" ]]; then
    return 0
  fi

  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -z "${line}" || "${line}" =~ ^[[:space:]]*# ]] && continue

    line="${line#export }"
    local key="${line%%=*}"
    local value="${line#*=}"

    key="${key#"${key%%[![:space:]]*}"}"
    key="${key%"${key##*[![:space:]]}"}"
    value="${value%$'\r'}"

    if [[ "${value}" =~ ^\".*\"$ || "${value}" =~ ^\'.*\'$ ]]; then
      value="${value:1:${#value}-2}"
    fi

    if [[ "${key}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
      export "${key}=${value}"
    fi
  done < "${env_file}"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  load_env_file "${1:-.env}"
fi
