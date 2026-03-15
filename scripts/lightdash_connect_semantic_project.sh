#!/usr/bin/env bash
set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - <<'PY'
import base64
import hashlib
import hmac
import json
import subprocess
import urllib.parse
from pathlib import Path

import requests


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[len('export '):]
        if '=' not in line:
            continue
        key, value = line.split('=', 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


repo = Path.cwd()
env = load_env(repo / '.env')
lightdash_env = load_env(repo / 'bi/lightdash/.env')

required = ['DBT_BQ_DEV_PROJECT', 'DBT_BQ_DEV_DATASET', 'DBT_BQ_DEV_KEYFILE']
for key in required:
    if key not in env or not env[key]:
        raise SystemExit(f'Missing required env var in .env: {key}')

secret = lightdash_env.get('LIGHTDASH_SECRET')
if not secret:
    raise SystemExit('Missing LIGHTDASH_SECRET in bi/lightdash/.env')

manifest_path = repo / 'wolt_assignment_dbt' / 'target' / 'manifest.json'
if not manifest_path.exists():
    subprocess.run(['./scripts/dbt.sh', 'compile', '--target', 'dev'], check=True)
if not manifest_path.exists():
    raise SystemExit('manifest.json not found after compile')

manifest = manifest_path.read_text(encoding='utf-8')

sid = subprocess.check_output(
    [
        'docker',
        'exec',
        'lightdash_postgres',
        'psql',
        '-U',
        'lightdash',
        '-d',
        'lightdash',
        '-Atc',
        'select sid from sessions order by expired desc limit 1;',
    ],
    text=True,
).strip()

if not sid:
    raise SystemExit('No active Lightdash session found. Login at http://localhost:18080 first.')

signature = base64.b64encode(
    hmac.new(secret.encode(), sid.encode(), hashlib.sha256).digest()
).decode().rstrip('=')
cookie = urllib.parse.quote(f's:{sid}.{signature}', safe='')

session = requests.Session()
session.headers.update({'Cookie': f'connect.sid={cookie}', 'Content-Type': 'application/json'})

base_url = 'http://localhost:18080'
project_name = env.get('LIGHTDASH_PROJECT_NAME', 'Wolt Assignment Dev Semantic')
project_dataset = f"{env['DBT_BQ_DEV_DATASET']}_core"

projects_resp = session.get(f'{base_url}/api/v1/org/projects', timeout=30)
projects_resp.raise_for_status()
projects = projects_resp.json().get('results', [])
existing = next((p for p in projects if p.get('name') == project_name), None)

if existing:
    project_uuid = existing['projectUuid']
    project_resp = session.get(f'{base_url}/api/v1/projects/{project_uuid}', timeout=60)
    project_resp.raise_for_status()
    current_project = project_resp.json()['results']
    # Lightdash expects a near-full project payload on PATCH.
    update_payload = {
        'name': current_project['name'],
        'type': current_project.get('type', 'DEFAULT'),
        'dbtConnection': {
            'type': 'manifest',
            'manifest': manifest,
            'hideRefreshButton': False,
        },
        'dbtVersion': 'v1.11',
        'warehouseConnection': current_project['warehouseConnection'],
    }
    update = session.patch(
        f'{base_url}/api/v1/projects/{project_uuid}',
        data=json.dumps(update_payload),
        timeout=120,
    )
    update.raise_for_status()
    # Refresh explores after manifest update.
    refresh = session.post(f'{base_url}/api/v1/projects/{project_uuid}/refresh', timeout=120)
    refresh.raise_for_status()
    action = 'Reused + updated manifest + refreshed'
else:
    keyfile = json.loads(Path(env['DBT_BQ_DEV_KEYFILE']).read_text(encoding='utf-8'))
    payload = {
        'name': project_name,
        'type': 'DEFAULT',
        'dbtConnection': {
            'type': 'manifest',
            'manifest': manifest,
            'hideRefreshButton': False,
        },
        'dbtVersion': 'v1.11',
        'warehouseConnection': {
            'type': 'bigquery',
            'authenticationType': 'private_key',
            'project': env['DBT_BQ_DEV_PROJECT'],
            'dataset': project_dataset,
            'location': env.get('DBT_BQ_LOCATION', 'EU'),
            'priority': 'interactive',
            'threads': 4,
            'timeoutSeconds': 300,
            'keyfileContents': keyfile,
        },
    }
    create_resp = session.post(
        f'{base_url}/api/v1/org/projects',
        data=json.dumps(payload),
        timeout=120,
    )
    create_resp.raise_for_status()
    project_uuid = create_resp.json()['results']['project']['projectUuid']
    action = 'Created'

print(f'{action} semantic project: {project_name} ({project_uuid})')
print(f'Open: http://localhost:18080/projects/{project_uuid}')
PY
