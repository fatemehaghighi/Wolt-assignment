#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
backup_root="${repo_root}/backups/lightdash"
ts="$(date +%Y%m%d_%H%M%S)"
out_dir="${backup_root}/${ts}"

mkdir -p "${out_dir}"

# Keep runtime config snapshot with each backup
cp -f "${repo_root}/bi/lightdash/.env" "${out_dir}/lightdash.env" 2>/dev/null || true
cp -f "${repo_root}/bi/lightdash/docker-compose.yml" "${out_dir}/docker-compose.yml" 2>/dev/null || true

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found" >&2
  exit 127
fi

# Full metadata backup (dashboards, charts, spaces, users, projects, sessions, etc.)
# Non-destructive: only reads DB and writes host files.
docker exec lightdash_postgres pg_dump \
  -U "${PGUSER:-lightdash}" \
  -d "${PGDATABASE:-lightdash}" \
  --clean --if-exists --no-owner --no-privileges \
  > "${out_dir}/lightdash_metadata.sql"

gzip -f "${out_dir}/lightdash_metadata.sql"

# Lightweight inventory for quick manual restore/inspection context
{
  echo "backup_timestamp=${ts}"
  echo "created_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "container=lightdash_postgres"
  echo "database=${PGDATABASE:-lightdash}"
  echo "user=${PGUSER:-lightdash}"
} > "${out_dir}/backup_info.txt"

docker exec lightdash_postgres psql \
  -U "${PGUSER:-lightdash}" \
  -d "${PGDATABASE:-lightdash}" \
  -Atc "
select
  (select count(*) from organizations) as organizations,
  (select count(*) from users) as users,
  (select count(*) from projects) as projects,
  (select count(*) from spaces) as spaces,
  (select count(*) from dashboards) as dashboards,
  (select count(*) from saved_queries) as saved_queries;
" > "${out_dir}/counts.txt" || true

# Export dashboard inventory (if schema exists as expected)
docker exec lightdash_postgres psql \
  -U "${PGUSER:-lightdash}" \
  -d "${PGDATABASE:-lightdash}" \
  -F $'\t' -Atc "
select d.dashboard_uuid, d.name, s.name as space_name, p.name as project_name
from dashboards d
left join spaces s on s.space_id = d.space_id
left join projects p on p.project_id = s.project_id
where d.deleted_at is null
order by p.name, s.name, d.name;
" > "${out_dir}/dashboards.tsv" || true

latest_link="${backup_root}/latest"
rm -f "${latest_link}" 2>/dev/null || true
ln -s "${out_dir}" "${latest_link}" || true

echo "Lightdash backup completed: ${out_dir}"
