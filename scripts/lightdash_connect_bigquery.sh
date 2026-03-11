#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - <<'PY'
import base64
import hashlib
import hmac
import json
import subprocess
import urllib.parse
from pathlib import Path

import requests

repo = Path.cwd()


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


def signed_cookie(secret: str, sid: str) -> str:
    signature = base64.b64encode(
        hmac.new(secret.encode(), sid.encode(), hashlib.sha256).digest()
    ).decode().rstrip('=')
    return urllib.parse.quote(f"s:{sid}.{signature}", safe='')


env = load_env(repo / '.env')
lightdash_env = load_env(repo / 'bi/lightdash/.env')

required = ['DBT_BQ_DEV_PROJECT', 'DBT_BQ_DEV_DATASET', 'DBT_BQ_DEV_KEYFILE']
for key in required:
    if key not in env or not env[key]:
        raise SystemExit(f"Missing required env var in .env: {key}")

secret = lightdash_env.get('LIGHTDASH_SECRET')
if not secret:
    raise SystemExit('Missing LIGHTDASH_SECRET in bi/lightdash/.env')

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
    raise SystemExit(
        'No active Lightdash session found. Login once at http://localhost:8080, then rerun this command.'
    )

cookie = signed_cookie(secret, sid)
headers = {'Cookie': f'connect.sid={cookie}', 'Content-Type': 'application/json'}
base_url = 'http://localhost:8080'

user_resp = requests.get(f'{base_url}/api/v1/user', headers=headers, timeout=20)
user_resp.raise_for_status()
user = user_resp.json()['results']

project_name = 'Wolt Assignment Dev'
project_dataset = f"{env['DBT_BQ_DEV_DATASET']}_core"

projects_resp = requests.get(f'{base_url}/api/v1/org/projects', headers=headers, timeout=20)
projects_resp.raise_for_status()
projects = projects_resp.json().get('results', [])
existing_project = next((p for p in projects if p.get('name') == project_name), None)

if existing_project:
    project_uuid = existing_project['projectUuid']
    print(f"Reusing Lightdash project: {project_name} ({project_uuid})")
else:
    keyfile = json.loads(Path(env['DBT_BQ_DEV_KEYFILE']).read_text(encoding='utf-8'))
    payload = {
        'name': project_name,
        'type': 'DEFAULT',
        'dbtConnection': {'type': 'none'},
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
    create_resp = requests.post(
        f'{base_url}/api/v1/org/projects',
        headers=headers,
        data=json.dumps(payload),
        timeout=60,
    )
    create_resp.raise_for_status()
    project_uuid = create_resp.json()['results']['project']['projectUuid']
    print(f"Created Lightdash project: {project_name} ({project_uuid})")

# Quick smoke test: list available tables from SQL Runner.
tables_resp = requests.get(
    f'{base_url}/api/v1/projects/{project_uuid}/sqlRunner/tables',
    headers=headers,
    timeout=30,
)
tables_resp.raise_for_status()

print(f"Authenticated as: {user['email']}")
print(f"Connected BigQuery project: {env['DBT_BQ_DEV_PROJECT']}")
print(f"Lightdash project UUID: {project_uuid}")
print('SQL Runner table discovery: OK')
print(f'Open: http://localhost:8080/projects/{project_uuid}')
PY
