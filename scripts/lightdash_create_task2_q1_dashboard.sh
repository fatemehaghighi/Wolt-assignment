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
        raise RuntimeError(f"Lightdash project '{project_name}' not found. Run 'make lightdash-connect-semantic' first.")
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
) -> str:
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
                'versionedData': {'sql': sql, 'config': config, 'limit': saved.get('limit') or 5000},
                'unversionedData': {'name': name, 'description': description, 'spaceUuid': saved['space']['uuid']},
            },
            timeout=30,
        )
        patch.raise_for_status()
        return saved_sql_uuid
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
    return create.json()['results']['savedSqlUuid']


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


def create_or_replace_dashboard(
    session: requests.Session,
    base_url: str,
    project_uuid: str,
    space_uuid: str,
    dashboard_name: str,
    description: str,
    charts: list[dict],
) -> str:
    existing_resp = session.get(f'{base_url}/api/v1/projects/{project_uuid}/dashboards', timeout=20)
    existing_resp.raise_for_status()
    existing = next((d for d in existing_resp.json()['results'] if d.get('name') == dashboard_name), None)
    if existing:
        delete_resp = session.delete(
            f"{base_url}/api/v1/projects/{project_uuid}/dashboards/{existing['uuid']}",
            timeout=20,
        )
        if delete_resp.status_code not in (200, 204, 404):
            delete_resp.raise_for_status()

    tab_uuid = str(uuid.uuid4())
    tiles = []
    y = 0
    for chart in charts:
        tiles.append(
            {
                'x': 0,
                'y': y,
                'w': 24,
                'h': 10,
                'type': 'sql_chart',
                'tabUuid': tab_uuid,
                'properties': {'chartName': chart['name'], 'savedSqlUuid': chart['savedSqlUuid']},
            }
        )
        tiles.append(
            {
                'x': 24,
                'y': y,
                'w': 24,
                'h': 10,
                'type': 'markdown',
                'tabUuid': tab_uuid,
                'properties': {'title': f"Guide: {chart['name']}", 'content': chart['guide_md']},
            }
        )
        y += 10

    payload = {
        'name': dashboard_name,
        'description': description,
        'spaceUuid': space_uuid,
        'tabs': [{'uuid': tab_uuid, 'name': 'Q1', 'order': 0}],
        'tiles': tiles,
    }
    created = session.post(
        f'{base_url}/api/v1/projects/{project_uuid}/dashboards',
        json=payload,
        timeout=30,
    )
    created.raise_for_status()
    return created.json()['results']['uuid']


def guide(what_it_says: str, main_metric: str, metric_calc: str, how_to_use: str) -> str:
    return (
        f"**What this chart says**\n\n- {what_it_says}\n\n"
        f"**Main metric**\n\n- {main_metric}\n\n"
        f"**How metric is calculated**\n\n- {metric_calc}\n\n"
        f"**How to use this chart**\n\n- {how_to_use}"
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

session = requests.Session()
session.headers.update({'Cookie': f"connect.sid={signed_cookie(secret, sid)}", 'Content-Type': 'application/json'})
base_url = 'http://localhost:18080'

project_name = os.environ.get('LIGHTDASH_PROJECT_NAME') or env.get('LIGHTDASH_PROJECT_NAME', 'Wolt Assignment Dev Semantic')
project_uuid = ensure_project(session, base_url, project_name)
space_uuid = ensure_space(session, base_url, project_uuid, 'Task 2 Visuals')

dev_project = env['DBT_BQ_DEV_PROJECT']
dev_dataset = env['DBT_BQ_DEV_DATASET']

charts_config = [
    {
        'slug': 'task2-q1-category-monthly-performance',
        'name': 'Q1 Category Monthly Performance',
        'description': 'Monthly revenue/units/growth and promo/repeat mix by category.',
        'config': {
            'type': 'line',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'time', 'reference': 'period_month'},
                'y': [{'aggregation': 'any', 'reference': 'revenue_eur'}],
                'groupBy': [{'reference': 'item_category'}],
            },
            'display': {},
        },
        'sql': f"""
with base as (
  select
    order_month as period_month,
    item_category,
    order_item_rows_revenue_eur as revenue_eur,
    units_sold,
    orders as orders_with_category,
    promo_units_sold,
    distinct_customers_whose_first_order_included_category_in_month as first_order_customers_attr,
    distinct_customers_with_repeat_orders_including_category_in_month as repeat_order_customers_attr,
    weighted_avg_selling_price_eur as weighted_asp_eur
  from `{dev_project}.{dev_dataset}_rpt.rpt_category_monthly`
  where order_month between date '2023-01-01' and date '2023-12-01'
),
with_lag as (
  select
    *,
    lag(revenue_eur) over(partition by item_category order by period_month) as prev_revenue_eur
  from base
)
select
  period_month,
  item_category,
  revenue_eur,
  units_sold,
  orders_with_category,
  weighted_asp_eur,
  safe_divide(promo_units_sold, nullif(units_sold, 0)) as promo_unit_share,
  safe_divide(repeat_order_customers_attr, nullif(first_order_customers_attr + repeat_order_customers_attr, 0)) as repeat_customer_mix_ratio,
  safe_divide(revenue_eur - prev_revenue_eur, nullif(prev_revenue_eur, 0)) as revenue_mom_growth_ratio
from with_lag
order by item_category, period_month
""",
        'guide_md': guide(
            'Trend of each category over 2023 and mix quality signals.',
            'Revenue EUR and MoM revenue growth ratio.',
            'Revenue = SUM(order_item_rows_revenue_eur); growth = (current - previous) / previous by category-month.',
            'Use this first to identify which categories are structurally growing vs unstable.',
        ),
    },
    {
        'slug': 'task2-q1-category-scorecard-controls',
        'name': 'Q1 Category Scorecard And Controls',
        'description': 'Category ranking with volatility, concentration and promo dependency controls.',
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'item_category': {'visible': True},
                'total_revenue_eur': {'visible': True},
                'revenue_growth_ratio_first_to_last': {'visible': True},
                'promo_unit_share': {'visible': True},
                'repeat_customer_mix_ratio': {'visible': True},
                'star_item_name': {'visible': True},
                'star_item_revenue_share': {'visible': True},
                'control_high_star_concentration_flag': {'visible': True},
                'control_high_revenue_volatility_flag': {'visible': True},
                'control_high_promo_dependency_flag': {'visible': True},
                'control_low_repeat_mix_flag': {'visible': True},
            },
        },
        'sql': f"""
with month_category as (
  select
    order_month as period_month,
    item_category,
    order_item_rows_revenue_eur as revenue_eur,
    units_sold,
    orders as orders_with_category,
    promo_units_sold,
    distinct_customers_whose_first_order_included_category_in_month as first_order_customers_attr,
    distinct_customers_with_repeat_orders_including_category_in_month as repeat_order_customers_attr
  from `{dev_project}.{dev_dataset}_rpt.rpt_category_monthly`
  where order_month between date '2023-01-01' and date '2023-12-01'
),
by_cat as (
  select
    item_category,
    min(period_month) as first_month,
    max(period_month) as last_month,
    sum(revenue_eur) as total_revenue_eur,
    sum(units_sold) as total_units_sold,
    sum(orders_with_category) as total_orders_with_category,
    safe_divide(sum(promo_units_sold), nullif(sum(units_sold), 0)) as promo_unit_share,
    safe_divide(sum(repeat_order_customers_attr), nullif(sum(first_order_customers_attr + repeat_order_customers_attr), 0)) as repeat_customer_mix_ratio,
    avg(revenue_eur) as avg_monthly_revenue_eur,
    stddev_pop(revenue_eur) as std_monthly_revenue_eur
  from month_category
  group by 1
),
first_last as (
  select
    m.item_category,
    max(case when m.period_month = b.first_month then m.revenue_eur end) as first_month_revenue_eur,
    max(case when m.period_month = b.last_month then m.revenue_eur end) as last_month_revenue_eur
  from month_category m
  join by_cat b using(item_category)
  group by 1
),
star as (
  select
    item_category,
    item_key,
    any_value(item_name_preferred) as item_name_preferred,
    sum(order_item_row_final_amount_gross_eur) as item_revenue_eur,
    row_number() over(partition by item_category order by sum(order_item_row_final_amount_gross_eur) desc) as rn
  from `{dev_project}.{dev_dataset}_core.fct_order_item`
  where order_date between date '2023-01-01' and date '2023-12-31'
  group by 1,2
)
select
  b.item_category,
  b.total_revenue_eur,
  safe_divide(fl.last_month_revenue_eur - fl.first_month_revenue_eur, nullif(fl.first_month_revenue_eur, 0)) as revenue_growth_ratio_first_to_last,
  b.promo_unit_share,
  b.repeat_customer_mix_ratio,
  s.item_name_preferred as star_item_name,
  safe_divide(s.item_revenue_eur, nullif(b.total_revenue_eur, 0)) as star_item_revenue_share,
  case when safe_divide(s.item_revenue_eur, nullif(b.total_revenue_eur, 0)) >= 0.65 then 1 else 0 end as control_high_star_concentration_flag,
  case when safe_divide(b.std_monthly_revenue_eur, nullif(b.avg_monthly_revenue_eur, 0)) >= 0.60 then 1 else 0 end as control_high_revenue_volatility_flag,
  case when b.promo_unit_share >= 0.12 then 1 else 0 end as control_high_promo_dependency_flag,
  case when b.repeat_customer_mix_ratio <= 0.95 then 1 else 0 end as control_low_repeat_mix_flag
from by_cat b
join first_last fl using(item_category)
left join star s on b.item_category = s.item_category and s.rn = 1
order by b.total_revenue_eur desc
""",
        'guide_md': guide(
            'Single scorecard for category quality and risk controls.',
            'Total revenue, first-to-last growth, star concentration, volatility, promo dependency.',
            'Flags are threshold-based controls; they do not block data, they surface risk.',
            'Use this for business review and prioritize categories with multiple red flags.',
        ),
    },
    {
        'slug': 'task2-q1-star-products-detail',
        'name': 'Q1 Star Products Detail',
        'description': 'Top 10 products per category with revenue and share.',
        'config': {
            'type': 'vertical_bar',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'category', 'reference': 'item_name_preferred'},
                'y': [{'aggregation': 'any', 'reference': 'revenue_eur'}],
                'groupBy': [{'reference': 'item_category'}],
            },
            'display': {},
        },
        'sql': f"""
with base as (
  select
    item_category,
    item_key,
    any_value(item_name_preferred) as item_name_preferred,
    sum(order_item_row_final_amount_gross_eur) as revenue_eur,
    sum(units_in_order_item_row) as units_sold,
    count(distinct order_sk) as orders_count
  from `{dev_project}.{dev_dataset}_core.fct_order_item`
  where order_date between date '2023-01-01' and date '2023-12-31'
  group by 1,2
),
cat_totals as (
  select item_category, sum(revenue_eur) as category_revenue_eur, sum(units_sold) as category_units_sold
  from base
  group by 1
)
select
  b.item_category,
  b.item_key,
  b.item_name_preferred,
  b.revenue_eur,
  b.units_sold,
  b.orders_count,
  safe_divide(b.revenue_eur, nullif(c.category_revenue_eur, 0)) as revenue_share_in_category,
  safe_divide(b.units_sold, nullif(c.category_units_sold, 0)) as unit_share_in_category,
  row_number() over(partition by b.item_category order by b.revenue_eur desc) as revenue_rank_in_category
from base b
join cat_totals c using(item_category)
qualify revenue_rank_in_category <= 10
order by item_category, revenue_rank_in_category
""",
        'guide_md': guide(
            'Identifies star SKUs inside each category, not just category totals.',
            'Product revenue and revenue share within category.',
            'Revenue share = product revenue / total category revenue for 2023.',
            'Use for assortment decisions, shelf priority, and dependency risk review.',
        ),
    },
]

chart_results: list[dict] = []
for c in charts_config:
    saved_uuid = upsert_sql_chart(
        session=session,
        base_url=base_url,
        project_uuid=project_uuid,
        space_uuid=space_uuid,
        slug=c['slug'],
        name=c['name'],
        description=c['description'],
        sql=c['sql'],
        config=c['config'],
    )
    chart_results.append({'name': c['name'], 'savedSqlUuid': saved_uuid, 'guide_md': c['guide_md']})

dashboard_uuid = create_or_replace_dashboard(
    session=session,
    base_url=base_url,
    project_uuid=project_uuid,
    space_uuid=space_uuid,
    dashboard_name='Task 2 - Q1 Category Performance Deep Dive',
    description='Q1 deep-dive dashboard for Task 2: best categories, star products, and control flags.',
    charts=chart_results,
)

print(json.dumps({
    'project_uuid': project_uuid,
    'space_uuid': space_uuid,
    'dashboard_uuid': dashboard_uuid,
    'url': f'http://localhost:18080/projects/{project_uuid}/dashboards/{dashboard_uuid}/view'
}, indent=2))
PY
