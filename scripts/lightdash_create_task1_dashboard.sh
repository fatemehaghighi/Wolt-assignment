#!/usr/bin/env bash
set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 - <<'PY'
import base64
import hashlib
import hmac
import json
import os
import subprocess
import urllib.parse
import uuid
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


def signed_cookie(secret: str, sid: str) -> str:
    signature = base64.b64encode(
        hmac.new(secret.encode(), sid.encode(), hashlib.sha256).digest()
    ).decode().rstrip('=')
    return urllib.parse.quote(f"s:{sid}.{signature}", safe='')


def ensure_project(session: requests.Session, base_url: str, project_name: str) -> str:
    r = session.get(f'{base_url}/api/v1/org/projects', timeout=20)
    r.raise_for_status()
    projects = r.json().get('results', [])
    p = next((x for x in projects if x.get('name') == project_name), None)
    if not p:
        raise RuntimeError(
            f"Lightdash project '{project_name}' not found. Run 'make lightdash-connect-semantic' first."
        )
    return p['projectUuid']


def ensure_space(session: requests.Session, base_url: str, project_uuid: str, space_name: str) -> str:
    r = session.get(f'{base_url}/api/v1/projects/{project_uuid}/spaces', timeout=20)
    r.raise_for_status()
    spaces = r.json().get('results', [])
    found = next((s for s in spaces if s.get('name') == space_name), None)
    if found:
        return found['uuid']

    create = session.post(
        f'{base_url}/api/v1/projects/{project_uuid}/spaces',
        json={'name': space_name},
        timeout=20,
    )
    create.raise_for_status()
    return create.json()['results']['uuid']


def upsert_sql_chart(
    session: requests.Session,
    base_url: str,
    project_uuid: str,
    space_uuid: str,
    slug: str,
    name: str,
    description: str,
    sql: str,
    config: dict,
) -> tuple[str, str]:
    config = normalize_table_config(config)
    get = session.get(
        f'{base_url}/api/v1/projects/{project_uuid}/sqlRunner/saved/slug/{slug}',
        timeout=20,
    )

    if get.status_code == 200:
        saved = get.json()['results']
        saved_sql_uuid = saved['savedSqlUuid']
        patch = session.patch(
            f'{base_url}/api/v1/projects/{project_uuid}/sqlRunner/saved/{saved_sql_uuid}',
            json={
                'versionedData': {
                    'sql': sql,
                    'config': config,
                    'limit': saved.get('limit') or 5000,
                },
                'unversionedData': {
                    'name': name,
                    'description': description,
                    'spaceUuid': saved['space']['uuid'],
                },
            },
            timeout=30,
        )
        patch.raise_for_status()
        return saved_sql_uuid, slug

    if get.status_code != 404:
        get.raise_for_status()

    create = session.post(
        f'{base_url}/api/v1/projects/{project_uuid}/sqlRunner/saved',
        json={
            'slug': slug,
            'spaceUuid': space_uuid,
            'name': name,
            'description': description,
            'sql': sql,
            'config': config,
            'limit': 5000,
        },
        timeout=30,
    )
    create.raise_for_status()
    result = create.json()['results']
    return result['savedSqlUuid'], result['slug']


def normalize_table_config(config: dict) -> dict:
    if config.get('type') != 'table':
        return config
    columns = config.get('columns', {})
    normalized: dict[str, dict] = {}
    for order, (col_name, col_cfg) in enumerate(columns.items()):
        visible = bool(col_cfg.get('visible', True)) if isinstance(col_cfg, dict) else True
        normalized[col_name] = {
            'reference': col_name,
            'label': col_name,
            'frozen': False,
            'visible': visible,
            'order': order,
        }
    out = dict(config)
    out['columns'] = normalized
    return out


def enforce_table_columns(saved_sql_uuid: str, config: dict) -> None:
    config = normalize_table_config(config)
    config_json = json.dumps(config, separators=(',', ':'))
    subprocess.run(
        [
            'docker',
            'exec',
            '-i',
            'lightdash_postgres',
            'psql',
            '-U',
            'lightdash',
            '-d',
            'lightdash',
            '-c',
            (
                "update saved_sql_versions "
                f"set config = '{config_json}'::jsonb "
                f"where saved_sql_uuid = '{saved_sql_uuid}' "
                "and created_at = ("
                "  select max(created_at) from saved_sql_versions "
                f"  where saved_sql_uuid = '{saved_sql_uuid}'"
                ");"
            ),
        ],
        check=True,
        text=True,
        capture_output=True,
    )


def create_dashboard(
    session: requests.Session,
    base_url: str,
    project_uuid: str,
    dashboard_name: str,
    description: str,
    charts: list[dict],
) -> str:
    tab_uuid = str(uuid.uuid4())
    tiles = []
    y = 0
    chart_w = int(os.environ.get('LIGHTDASH_CHART_TILE_WIDTH', '24'))
    if chart_w < 12 or chart_w > 36:
        chart_w = 24
    guide_w = 48 - chart_w
    tile_h = int(os.environ.get('LIGHTDASH_ROW_TILE_HEIGHT', '10'))
    if tile_h < 6 or tile_h > 18:
        tile_h = 10
    for chart in charts:
        tiles.append(
            {
                'x': 0,
                'y': y,
                'w': chart_w,
                'h': tile_h,
                'type': 'sql_chart',
                'tabUuid': tab_uuid,
                'properties': {
                    'chartName': chart['name'],
                    'savedSqlUuid': chart['savedSqlUuid'],
                },
            }
        )
        tiles.append(
            {
                'x': chart_w,
                'y': y,
                'w': guide_w,
                'h': tile_h,
                'type': 'markdown',
                'tabUuid': tab_uuid,
                'properties': {
                    'title': f"Guide: {chart['name']}",
                    'content': chart['guide_md'],
                },
            }
        )
        y += tile_h

    payload = {
        'name': dashboard_name,
        'description': description,
        'spaceUuid': charts[0].get('spaceUuid') if charts else None,
        'tabs': [{'uuid': tab_uuid, 'name': 'Task 1', 'order': 0}],
        'tiles': tiles,
    }

    existing_resp = session.get(
        f'{base_url}/api/v1/projects/{project_uuid}/dashboards', timeout=20
    )
    existing_resp.raise_for_status()
    existing = next(
        (d for d in existing_resp.json()['results'] if d.get('name') == dashboard_name),
        None,
    )
    if existing:
        # Keep manual UI layout edits by default.
        # Set LIGHTDASH_RECREATE_DASHBOARD=1 when you want to force full reset.
        if os.environ.get('LIGHTDASH_RECREATE_DASHBOARD', '0') != '1':
            return existing['uuid']
        session.delete(
            f"{base_url}/api/v1/projects/{project_uuid}/dashboards/{existing['uuid']}",
            timeout=20,
        )

    created = session.post(
        f'{base_url}/api/v1/projects/{project_uuid}/dashboards',
        json=payload,
        timeout=30,
    )
    created.raise_for_status()
    return created.json()['results']['uuid']


def build_guide_md(meta: dict, description: str) -> str:
    return (
        f"**What this chart says**\n- {meta['what_it_says'] or description}\n\n"
        f"**Main metric**\n- {meta['main_metric']}\n\n"
        f"**How metric is calculated**\n- {meta['metric_calc']}\n\n"
        f"**How to use this chart**\n- {meta['how_to_use']}"
    )


repo = Path.cwd()
env = load_env(repo / '.env')
lightdash_env = load_env(repo / 'bi/lightdash/.env')

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
    raise SystemExit('No active Lightdash session found. Login at http://localhost:18080 first.')

cookie = signed_cookie(secret, sid)
session = requests.Session()
session.headers.update(
    {
        'Cookie': f'connect.sid={cookie}',
        'Content-Type': 'application/json',
    }
)

base_url = 'http://localhost:18080'
user_resp = session.get(f'{base_url}/api/v1/user', timeout=20)
user_resp.raise_for_status()
user = user_resp.json()['results']

project_name = env.get('LIGHTDASH_PROJECT_NAME', 'Wolt Assignment Dev Semantic')
project_uuid = ensure_project(session, base_url, project_name)
space_uuid = ensure_space(session, base_url, project_uuid, 'Task 1 Visuals')

dev_project = env['DBT_BQ_DEV_PROJECT']
dev_dataset = env['DBT_BQ_DEV_DATASET']

queries = [
    {
        'slug': 'task1-live-insight-summary',
        'name': 'Live Insight Summary (Auto-Updated)',
        'description': 'Automatically refreshed operational/commercial highlights from latest order data.',
        'note_md': (
            '**Auto insight**\n'
            '- Generated directly from latest fact data at each refresh.\n\n'
            '**How to use**\n'
            '- Use as executive headline, then drill down in charts below.'
        ),
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'insight_name': {'visible': True},
                'insight_value': {'visible': True},
            },
        },
        'sql': f"""
with latest_month as (
  select max(date_trunc(order_date, month)) as period_month
  from `{dev_project}.{dev_dataset}_core.fct_order`
), by_category as (
  select
    item_category,
    sum(units_in_order_item_row) as units_sold,
    row_number() over(order by sum(units_in_order_item_row) desc) as rn
  from `{dev_project}.{dev_dataset}_core.fct_order_item`
  where date_trunc(order_date, month) = (select period_month from latest_month)
  group by 1
), monthly_orders as (
  select
    count(*) as orders_count,
    safe_divide(sum(cast(has_any_promo_units_in_order as int64)), count(*)) as promo_order_share
  from `{dev_project}.{dev_dataset}_core.fct_order`
  where date_trunc(order_date, month) = (select period_month from latest_month)
)
select
  'Top category by units (latest month)' as insight_name,
  coalesce(max(case when rn = 1 then item_category end), 'n/a') as insight_value
from by_category
union all
select
  'Orders in latest month' as insight_name,
  cast(orders_count as string) as insight_value
from monthly_orders
union all
select
  'Promo order share (latest month)' as insight_name,
  cast(round(coalesce(promo_order_share, 0) * 100, 2) as string)
from monthly_orders
""",
    },
    {
        'slug': 'task1-service-area-by-month',
        'name': 'Q1 Service Area Coverage (Monthly)',
        'description': 'Service-area proxy using delivery distance distribution by month (avg / p50 / p90).',
        'note_md': (
            '**What this shows**\n'
            '- Monthly service-area proxy via delivery distance distribution.\n\n'
            '**Insight**\n'
            '- Rising p90 distance with stable order count can indicate serving radius expansion.'
        ),
        'config': {
            'type': 'line',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'time', 'reference': 'period_month'},
                'y': [
                    {'aggregation': 'any', 'reference': 'avg_delivery_distance_m'},
                    {'aggregation': 'any', 'reference': 'p50_delivery_distance_m'},
                    {'aggregation': 'any', 'reference': 'p90_delivery_distance_m'},
                ],
            },
            'display': {},
        },
        'sql': f"""
select
  date_trunc(order_date, month) as period_month,
  count(*) as orders_count,
  round(avg(delivery_distance_line_meters), 2) as avg_delivery_distance_m,
  approx_quantiles(delivery_distance_line_meters, 100)[offset(50)] as p50_delivery_distance_m,
  approx_quantiles(delivery_distance_line_meters, 100)[offset(90)] as p90_delivery_distance_m
from `{dev_project}.{dev_dataset}_core.fct_order`
group by 1
order by 1
""",
        'insight_sql': f"""
with monthly as (
  select
    date_trunc(order_date, month) as period_month,
    avg(delivery_distance_line_meters) as avg_delivery_distance_m
  from `{dev_project}.{dev_dataset}_core.fct_order`
  group by 1
)
select
  'Service area trend' as insight_title,
  concat('latest avg distance: ', cast(round(avg_delivery_distance_m, 2) as string), ' m') as insight_value,
  'based on latest month' as insight_comment
from monthly
order by period_month desc
limit 1
""",
    },
    {
        'slug': 'task1-items-and-prices-by-month',
        'name': 'Q2 Items Bought + Selling Price (Monthly)',
        'description': 'Items and effective unit selling price over time (after discounts).',
        'note_md': (
            '**What this shows**\n'
            '- Monthly units sold by category.\n\n'
            '**Insight**\n'
            '- High-volume categories drive growth; compare against price trend to spot mix shifts.'
        ),
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'period_month': {'visible': True},
                'item_category': {'visible': True},
                'units_sold': {'visible': True},
                'weighted_avg_unit_selling_price_eur': {'visible': True},
            },
        },
        'sql': f"""
select
  date_trunc(order_date, month) as period_month,
  item_category,
  sum(units_in_order_item_row) as units_sold,
  safe_divide(sum(order_item_row_final_amount_gross_eur), nullif(sum(units_in_order_item_row), 0)) as weighted_avg_unit_selling_price_eur
from `{dev_project}.{dev_dataset}_core.fct_order_item`
group by 1,2
order by 1, units_sold desc
""",
        'insight_sql': f"""
with monthly as (
  select
    date_trunc(order_date, month) as period_month,
    item_category,
    sum(units_in_order_item_row) as units_sold
  from `{dev_project}.{dev_dataset}_core.fct_order_item`
  group by 1,2
), latest as (
  select * from monthly where period_month = (select max(period_month) from monthly)
)
select
  'Top category by units (latest month)' as insight_title,
  item_category as insight_value,
  concat('units=', cast(units_sold as string)) as insight_comment
from latest
order by units_sold desc
limit 1
""",
    },
    {
        'slug': 'task1-promo-uptake-by-month',
        'name': 'Q3/Q4 Promo Uptake (Monthly)',
        'description': 'Promo units and promo-order adoption share by month.',
        'note_md': (
            '**What this shows**\n'
            '- Share of promo units and share of orders containing promo items.\n\n'
            '**Insight**\n'
            '- If promo order share rises but promo unit share is flat, promos are likely basket-entry levers.'
        ),
        'config': {
            'type': 'line',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'time', 'reference': 'period_month'},
                'y': [
                    {'aggregation': 'any', 'reference': 'promo_unit_share'},
                    {'aggregation': 'any', 'reference': 'promo_order_share'},
                ],
            },
            'display': {},
        },
        'sql': f"""
with unit_rollup as (
  select
    date_trunc(order_date, month) as period_month,
    sum(case when is_promo_item then units_in_order_item_row else 0 end) as promo_units,
    sum(units_in_order_item_row) as total_units
  from `{dev_project}.{dev_dataset}_core.fct_order_item`
  group by 1
), order_rollup as (
  select
    date_trunc(order_date, month) as period_month,
    sum(cast(has_any_promo_units_in_order as int64)) as orders_with_any_promo,
    count(*) as total_orders
  from `{dev_project}.{dev_dataset}_core.fct_order`
  group by 1
)
select
  u.period_month,
  u.promo_units,
  u.total_units,
  safe_divide(u.promo_units, nullif(u.total_units, 0)) as promo_unit_share,
  o.orders_with_any_promo,
  o.total_orders,
  safe_divide(o.orders_with_any_promo, nullif(o.total_orders, 0)) as promo_order_share
from unit_rollup u
join order_rollup o using(period_month)
order by 1
""",
        'insight_sql': f"""
with unit_rollup as (
  select
    date_trunc(order_date, month) as period_month,
    sum(case when is_promo_item then units_in_order_item_row else 0 end) as promo_units,
    sum(units_in_order_item_row) as total_units
  from `{dev_project}.{dev_dataset}_core.fct_order_item`
  group by 1
)
select
  'Promo unit share (latest month)' as insight_title,
  concat(cast(round(safe_divide(promo_units, nullif(total_units, 0)) * 100, 2) as string), '%') as insight_value,
  'share of units bought on promo' as insight_comment
from unit_rollup
order by period_month desc
limit 1
""",
    },
    {
        'slug': 'task1-customer-repeat-by-month',
        'name': 'Q5 Customer Return Behavior (Monthly)',
        'description': 'First-time vs repeat order mix by month.',
        'note_md': (
            '**What this shows**\n'
            '- Split of first-time and repeat orders.\n\n'
            '**Insight**\n'
            '- Repeat share trend is a direct signal of early retention quality.'
        ),
        'config': {
            'type': 'vertical_bar',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'time', 'reference': 'period_month'},
                'y': [
                    {'aggregation': 'any', 'reference': 'first_orders'},
                    {'aggregation': 'any', 'reference': 'repeat_orders'},
                ],
                'stack': 'stack',
            },
            'display': {},
        },
        'sql': f"""
select
  date_trunc(order_date, month) as period_month,
  sum(cast(is_first_order_for_customer as int64)) as first_orders,
  sum(cast(not is_first_order_for_customer as int64)) as repeat_orders,
  safe_divide(sum(cast(not is_first_order_for_customer as int64)), count(*)) as repeat_order_share
from `{dev_project}.{dev_dataset}_core.fct_order`
group by 1
order by 1
""",
        'insight_sql': f"""
with monthly as (
  select
    date_trunc(order_date, month) as period_month,
    safe_divide(sum(cast(not is_first_order_for_customer as int64)), count(*)) as repeat_order_share
  from `{dev_project}.{dev_dataset}_core.fct_order`
  group by 1
)
select
  'Repeat order share (latest month)' as insight_title,
  concat(cast(round(repeat_order_share * 100, 2) as string), '%') as insight_value,
  'customer return signal' as insight_comment
from monthly
order by period_month desc
limit 1
""",
    },
    {
        'slug': 'task1-fees-vs-basket-by-month',
        'name': 'Q6 Fees vs Basket Value (Monthly)',
        'description': 'Compares basket value against Wolt and courier fees.',
        'note_md': (
            '**What this shows**\n'
            '- Fee-to-basket ratios over time.\n\n'
            '**Insight**\n'
            '- Persistent ratio increase can hurt conversion and should be reviewed with ops/pricing.'
        ),
        'config': {
            'type': 'line',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'time', 'reference': 'period_month'},
                'y': [
                    {'aggregation': 'any', 'reference': 'service_fee_to_basket_ratio'},
                    {'aggregation': 'any', 'reference': 'courier_fee_to_basket_ratio'},
                ],
            },
            'display': {},
        },
        'sql': f"""
select
  date_trunc(order_date, month) as period_month,
  sum(total_basket_value_eur) as basket_value_eur,
  sum(wolt_service_fee_eur) as wolt_service_fee_eur,
  sum(courier_base_fee_eur) as courier_base_fee_eur,
  safe_divide(sum(wolt_service_fee_eur), nullif(sum(total_basket_value_eur), 0)) as service_fee_to_basket_ratio,
  safe_divide(sum(courier_base_fee_eur), nullif(sum(total_basket_value_eur), 0)) as courier_fee_to_basket_ratio
from `{dev_project}.{dev_dataset}_core.fct_order`
group by 1
order by 1
""",
        'insight_sql': f"""
with monthly as (
  select
    date_trunc(order_date, month) as period_month,
    safe_divide(sum(wolt_service_fee_eur), nullif(sum(total_basket_value_eur), 0)) as service_fee_to_basket_ratio
  from `{dev_project}.{dev_dataset}_core.fct_order`
  group by 1
)
select
  'Service fee / basket (latest month)' as insight_title,
  concat(cast(round(service_fee_to_basket_ratio * 100, 2) as string), '%') as insight_value,
  'pricing pressure indicator' as insight_comment
from monthly
order by period_month desc
limit 1
""",
    },
    {
        'slug': 'task1-revenue-components-by-month',
        'name': 'Q7 Revenue Components (Monthly)',
        'description': 'Customer-paid total plus its components (basket/service/courier).',
        'note_md': (
            '**What this shows**\n'
            '- Monthly paid total decomposed into basket value and fees.\n\n'
            '**Insight**\n'
            '- Helps explain revenue composition changes, not only top-line movement.'
        ),
        'config': {
            'type': 'vertical_bar',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'time', 'reference': 'period_month'},
                'y': [
                    {'aggregation': 'any', 'reference': 'basket_value_eur'},
                    {'aggregation': 'any', 'reference': 'wolt_service_fee_eur'},
                    {'aggregation': 'any', 'reference': 'courier_base_fee_eur'},
                ],
                'stack': 'stack',
            },
            'display': {},
        },
        'sql': f"""
select
  date_trunc(order_date, month) as period_month,
  sum(total_customer_paid_eur) as customer_paid_total_eur,
  sum(total_basket_value_eur) as basket_value_eur,
  sum(wolt_service_fee_eur) as wolt_service_fee_eur,
  sum(courier_base_fee_eur) as courier_base_fee_eur
from `{dev_project}.{dev_dataset}_core.fct_order`
group by 1
order by 1
""",
        'insight_sql': f"""
with monthly as (
  select
    date_trunc(order_date, month) as period_month,
    sum(total_customer_paid_eur) as customer_paid_total_eur
  from `{dev_project}.{dev_dataset}_core.fct_order`
  group by 1
)
select
  'Customer paid total (latest month)' as insight_title,
  cast(round(customer_paid_total_eur, 2) as string) as insight_value,
  'EUR' as insight_comment
from monthly
order by period_month desc
limit 1
""",
    },
    {
        'slug': 'task1-courier-cost-by-month',
        'name': 'Q8 Courier Costs (Monthly)',
        'description': 'Courier base fee totals and order-level average by month.',
        'note_md': (
            '**What this shows**\n'
            '- Total courier cost and average courier cost per order.\n\n'
            '**Insight**\n'
            '- Divergence between total and average can indicate volume growth vs per-order cost pressure.'
        ),
        'config': {
            'type': 'line',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'time', 'reference': 'period_month'},
                'y': [
                    {'aggregation': 'any', 'reference': 'total_courier_cost_eur'},
                    {'aggregation': 'any', 'reference': 'avg_courier_cost_per_order_eur'},
                ],
            },
            'display': {},
        },
        'sql': f"""
select
  date_trunc(order_date, month) as period_month,
  sum(courier_base_fee_eur) as total_courier_cost_eur,
  avg(courier_base_fee_eur) as avg_courier_cost_per_order_eur
from `{dev_project}.{dev_dataset}_core.fct_order`
group by 1
order by 1
""",
        'insight_sql': f"""
with monthly as (
  select
    date_trunc(order_date, month) as period_month,
    avg(courier_base_fee_eur) as avg_courier_cost_per_order_eur
  from `{dev_project}.{dev_dataset}_core.fct_order`
  group by 1
)
select
  'Avg courier cost per order (latest month)' as insight_title,
  cast(round(avg_courier_cost_per_order_eur, 2) as string) as insight_value,
  'EUR per order' as insight_comment
from monthly
order by period_month desc
limit 1
""",
    },
]

chart_results = []
guide_meta = {
    'task1-service-area-by-month': {
        'what_it_says': 'Shows service-area coverage proxy over time using delivery distance distribution.',
        'main_metric': 'Avg / p50 / p90 delivery distance (meters) by month.',
        'metric_calc': 'AVG(delivery_distance_line_meters), APPROX_QUANTILES(...)[50], APPROX_QUANTILES(...)[90] grouped by month from fct_order.',
        'how_to_use': 'If p90 rises while orders stay stable or grow, coverage radius likely expanded.',
    },
    'task1-items-and-prices-by-month': {
        'what_it_says': 'Shows both units sold and weighted average unit selling price by category per month.',
        'main_metric': 'Units sold and weighted avg unit selling price (EUR).',
        'metric_calc': 'Units = SUM(units_in_order_item_row); weighted avg unit selling price = SUM(order_item_row_final_amount_gross_eur)/SUM(units_in_order_item_row), grouped by month and category.',
        'how_to_use': 'Separate volume-driven growth from price/mix-driven growth by reading units and weighted price together.',
    },
    'task1-promo-uptake-by-month': {
        'what_it_says': 'Shows whether orders and units are becoming more promo-driven.',
        'main_metric': 'Promo order share (%) and promo unit share (%).',
        'metric_calc': 'Promo order share = SUM(has_any_promo_units_in_order)/COUNT(orders); promo unit share = SUM(promo_units_in_order)/SUM(total_units_in_order).',
        'how_to_use': 'Use to evaluate promo dependency and track shifts in customer promo behavior.',
    },
    'task1-customer-repeat-by-month': {
        'what_it_says': 'Tracks first-time versus repeat customer mix by month.',
        'main_metric': 'Repeat customer order share (%).',
        'metric_calc': 'Repeat share = SUM(CASE WHEN is_first_order_for_customer THEN 0 ELSE 1 END)/COUNT(orders).',
        'how_to_use': 'Rising repeat share indicates healthier retention, while falling share can indicate acquisition-heavy growth.',
    },
    'task1-fees-vs-basket-by-month': {
        'what_it_says': 'Compares basket value against service and courier fees over time.',
        'main_metric': 'Basket, service fee, and courier fee totals (EUR).',
        'metric_calc': 'SUM(total_basket_value_eur), SUM(wolt_service_fee_eur), SUM(courier_base_fee_eur) grouped by month.',
        'how_to_use': 'Assess fee structure versus basket economics and identify months with unusual fee-to-basket mix.',
    },
    'task1-revenue-components-by-month': {
        'what_it_says': 'Breaks customer-paid totals into basket + fee components.',
        'main_metric': 'Total customer paid (EUR).',
        'metric_calc': 'SUM(total_customer_paid_eur) plus component sums for basket, service fee, and courier fee by month.',
        'how_to_use': 'Use for top-line trend and to understand which component contributes most to growth.',
    },
    'task1-courier-cost-by-month': {
        'what_it_says': 'Shows total courier cost and cost per order trend.',
        'main_metric': 'Total courier base fee and average courier base fee per order.',
        'metric_calc': 'SUM(courier_base_fee_eur) and AVG(courier_base_fee_eur) grouped by month.',
        'how_to_use': 'Monitor delivery cost pressure and compare against basket growth and order volumes.',
    },
}

for q in queries:
    if q['slug'] == 'task1-live-insight-summary':
        continue
    saved_sql_uuid, final_slug = upsert_sql_chart(
        session=session,
        base_url=base_url,
        project_uuid=project_uuid,
        space_uuid=space_uuid,
        slug=q['slug'],
        name=q['name'],
        description=q['description'],
        sql=q['sql'],
        config=q['config'],
    )
    enforce_table_columns(saved_sql_uuid, q['config'])
    chart_results.append(
        {
            'name': q['name'],
            'savedSqlUuid': saved_sql_uuid,
            'slug': final_slug,
            'spaceUuid': space_uuid,
            'guide_md': build_guide_md(guide_meta.get(q['slug'], {
                'what_it_says': q['description'],
                'main_metric': 'See chart metric axis/values.',
                'metric_calc': 'Computed by the SQL query backing this chart.',
                'how_to_use': 'Use trend and segment comparison for decision-making.',
            }), q['description']),
        }
    )

dashboard_uuid = create_dashboard(
    session=session,
    base_url=base_url,
    project_uuid=project_uuid,
    dashboard_name='Task 1 - Dimensional Model Consumption Pack (Explained v5)',
    description=(
        'Business-ready view for assignment Task 1 questions: service area, item prices, promo uptake, '
        'repeat behavior, fees vs basket, revenue components, and courier costs.'
    ),
    charts=chart_results,
)

print(f"Authenticated as: {user['email']}")
print(f"Project UUID: {project_uuid}")
print(f"Space UUID: {space_uuid}")
print(f"Dashboard UUID: {dashboard_uuid}")
print(f"Open dashboard: http://localhost:18080/projects/{project_uuid}/dashboards/{dashboard_uuid}")
PY
