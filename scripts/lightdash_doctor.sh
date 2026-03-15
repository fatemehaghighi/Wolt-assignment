#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
min_free_gb="${LIGHTDASH_MIN_FREE_GB:-8}"
keep_backups="${LIGHTDASH_KEEP_BACKUPS:-20}"
fix_mode="${1:-}"

warn() { printf "[WARN] %s\n" "$*"; }
info() { printf "[INFO] %s\n" "$*"; }
ok() { printf "[OK] %s\n" "$*"; }
fail() { printf "[FAIL] %s\n" "$*"; exit 1; }

free_kb="$(df -Pk ~ | awk 'NR==2 {print $4}')"
free_gb="$((free_kb / 1024 / 1024))"

info "Free disk on host: ${free_gb} GB"
if (( free_gb < min_free_gb )); then
  warn "Free disk is below threshold (${min_free_gb} GB)."
  if [[ "${fix_mode}" != "--fix" ]]; then
    fail "Run: make lightdash-maintain (safe cleanup) and retry."
  fi
fi

if [[ -d "${HOME}/.colima" ]]; then
  colima_size="$(du -sh "${HOME}/.colima" 2>/dev/null | awk '{print $1}')"
  info "Colima VM size: ${colima_size}"
fi

if ! command -v docker >/dev/null 2>&1; then
  fail "Docker CLI not found."
fi

if ! docker info >/dev/null 2>&1; then
  fail "Docker daemon not running."
fi
ok "Docker daemon is reachable."

if [[ "${fix_mode}" == "--fix" ]]; then
  info "Running safe maintenance (no volume prune)."
  if docker ps --format '{{.Names}}' | rg -q '^lightdash_postgres$'; then
    "${repo_root}/scripts/lightdash_backup.sh" || warn "Backup skipped/failed; continuing maintenance."
  fi
  docker image prune -af || true
  docker container prune -f || true
  docker builder prune -af || true
fi

info "Docker disk summary:"
docker system df || true

if docker ps --format '{{.Names}}' | rg -q '^lightdash_app$'; then
  code="$(curl -sS -o /tmp/lightdash_health.json -w "%{http_code}" http://localhost:18080/api/v1/health || true)"
  if [[ "${code}" == "200" ]]; then
    ok "Lightdash health endpoint is OK (200)."
  else
    warn "Lightdash health endpoint returned ${code}."
  fi
else
  warn "lightdash_app container is not running."
fi

backup_root="${repo_root}/backups/lightdash"
if [[ -d "${backup_root}" ]]; then
  dirs=()
  while IFS= read -r d; do
    dirs+=("$d")
  done < <(find "${backup_root}" -mindepth 1 -maxdepth 1 -type d -name '20*' | sort)
  total="${#dirs[@]}"
  if (( total > keep_backups )); then
    to_delete=$((total - keep_backups))
    warn "Pruning ${to_delete} old backup folders (keeping latest ${keep_backups})."
    for ((i=0; i<to_delete; i++)); do
      rm -rf "${dirs[$i]}"
    done
  fi
fi

ok "Lightdash doctor completed."
