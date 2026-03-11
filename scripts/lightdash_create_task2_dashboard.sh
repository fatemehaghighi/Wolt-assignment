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
        raise RuntimeError(f"Lightdash project '{project_name}' not found. Run 'make lightdash-connect' first.")
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
    # Try lookup by slug first.
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
        'tabs': [
            {
                'uuid': tab_uuid,
                'name': 'Task 2',
                'order': 0,
            }
        ],
        'tiles': tiles,
    }

    # Keep manual UI layout edits by default. Recreate only when explicitly requested.
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
    if created.status_code >= 400:
        print('Dashboard create payload error:', created.status_code, created.text)
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
    raise SystemExit('No active Lightdash session found. Login at http://localhost:8080 first.')

cookie = signed_cookie(secret, sid)
session = requests.Session()
session.headers.update(
    {
        'Cookie': f'connect.sid={cookie}',
        'Content-Type': 'application/json',
    }
)

base_url = 'http://localhost:8080'
user_resp = session.get(f'{base_url}/api/v1/user', timeout=20)
user_resp.raise_for_status()
user = user_resp.json()['results']

project_name = env.get('LIGHTDASH_PROJECT_NAME', 'Wolt Assignment Dev Semantic')
project_uuid = ensure_project(session, base_url, project_name)
space_uuid = ensure_space(session, base_url, project_uuid, 'Task 2 Visuals')

dev_project = env['DBT_BQ_DEV_PROJECT']
dev_dataset = env['DBT_BQ_DEV_DATASET']

queries = [
    {
        'slug': 'task2-category-monthly-growth',
        'name': 'Q1 Category Monthly Growth',
        'description': 'Monthly revenue and units by category to identify best performers.',
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
with latest as (
  select max(snapshot_date) as snapshot_date
  from `{dev_project}.{dev_dataset}_rpt.rpt_category_daily`
)
select
  date_trunc(date_day, month) as period_month,
  item_category,
  sum(order_item_rows_revenue_eur) as revenue_eur,
  sum(units_sold) as units_sold,
  safe_divide(sum(order_item_rows_revenue_eur), nullif(sum(units_sold),0)) as weighted_avg_selling_price_eur
from `{dev_project}.{dev_dataset}_rpt.rpt_category_daily`
where snapshot_date = (select snapshot_date from latest)
group by 1,2
order by period_month, revenue_eur desc
""",
        'insight_sql': f"""
with latest as (
  select max(snapshot_date) as snapshot_date
  from `{dev_project}.{dev_dataset}_rpt.rpt_category_daily`
), monthly as (
  select
    date_trunc(date_day, month) as period_month,
    item_category,
    sum(order_item_rows_revenue_eur) as revenue_eur
  from `{dev_project}.{dev_dataset}_rpt.rpt_category_daily`
  where snapshot_date = (select snapshot_date from latest)
  group by 1,2
), latest_month as (
  select max(period_month) as period_month from monthly
), ranked as (
  select item_category, revenue_eur, row_number() over(order by revenue_eur desc) as rn
  from monthly
  where period_month = (select period_month from latest_month)
)
select
  'Top categories (latest month)' as insight_title,
  concat(
    coalesce(max(case when rn = 1 then item_category end), 'n/a'),
    ', ',
    coalesce(max(case when rn = 2 then item_category end), 'n/a'),
    ', ',
    coalesce(max(case when rn = 3 then item_category end), 'n/a')
  ) as insight_value,
  'Updates automatically after each refresh' as insight_comment
from ranked
""",
    },
    {
        'slug': 'task2-star-products-by-category',
        'name': 'Q1 Star Products By Category',
        'description': 'Top revenue products per category.',
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
select
  item_category,
  item_key,
  item_name_preferred,
  sum(order_item_row_final_amount_gross_eur) as revenue_eur,
  sum(units_in_order_item_row) as units_sold,
  row_number() over (
    partition by item_category
    order by sum(order_item_row_final_amount_gross_eur) desc
  ) as category_rank
from `{dev_project}.{dev_dataset}_core.fct_order_item`
group by 1,2,3
qualify category_rank <= 10
order by item_category, category_rank
""",
        'insight_sql': f"""
with ranked as (
  select
    item_category,
    item_name_preferred,
    sum(order_item_row_final_amount_gross_eur) as revenue_eur,
    row_number() over(
      partition by item_category
      order by sum(order_item_row_final_amount_gross_eur) desc
    ) as rn
  from `{dev_project}.{dev_dataset}_core.fct_order_item`
  group by 1,2
)
select
  'Top product per category (sample)' as insight_title,
  concat(item_category, ': ', item_name_preferred) as insight_value,
  cast(round(revenue_eur, 2) as string) as insight_comment
from ranked
where rn = 1
order by revenue_eur desc
limit 1
""",
    },
    {
        'slug': 'task2-declining-categories-mom',
        'name': 'Q2 Declining Categories MoM',
        'description': 'Categories with negative month-over-month revenue change.',
        'config': {
            'type': 'vertical_bar',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'category', 'reference': 'item_category'},
                'y': [{'aggregation': 'any', 'reference': 'revenue_mom_change_eur'}],
                'groupBy': [{'reference': 'period_month'}],
            },
            'display': {},
        },
        'sql': f"""
with latest as (
  select max(snapshot_date) as snapshot_date
  from `{dev_project}.{dev_dataset}_rpt.rpt_category_daily`
), monthly as (
  select
    date_trunc(date_day, month) as period_month,
    item_category,
    sum(order_item_rows_revenue_eur) as revenue_eur,
    sum(units_sold) as units_sold
  from `{dev_project}.{dev_dataset}_rpt.rpt_category_daily`
  where snapshot_date = (select snapshot_date from latest)
  group by 1,2
), lagged as (
  select
    *,
    lag(revenue_eur) over(partition by item_category order by period_month) as prev_revenue_eur
  from monthly
)
select
  period_month,
  item_category,
  revenue_eur,
  prev_revenue_eur,
  revenue_eur - prev_revenue_eur as revenue_mom_change_eur,
  units_sold
from lagged
where prev_revenue_eur is not null
  and revenue_eur < prev_revenue_eur
order by period_month desc, revenue_mom_change_eur asc
""",
        'insight_sql': f"""
with latest as (
  select max(snapshot_date) as snapshot_date
  from `{dev_project}.{dev_dataset}_rpt.rpt_category_daily`
), monthly as (
  select
    date_trunc(date_day, month) as period_month,
    item_category,
    sum(order_item_rows_revenue_eur) as revenue_eur
  from `{dev_project}.{dev_dataset}_rpt.rpt_category_daily`
  where snapshot_date = (select snapshot_date from latest)
  group by 1,2
), lagged as (
  select
    period_month,
    item_category,
    revenue_eur,
    lag(revenue_eur) over(partition by item_category order by period_month) as prev_revenue_eur
  from monthly
), declined as (
  select
    period_month,
    item_category,
    revenue_eur - prev_revenue_eur as mom_change_eur
  from lagged
  where prev_revenue_eur is not null
    and revenue_eur < prev_revenue_eur
)
select
  'Biggest declining category' as insight_title,
  concat(item_category, ' @ ', cast(period_month as string)) as insight_value,
  concat('MoM change EUR: ', cast(round(mom_change_eur, 2) as string)) as insight_comment
from declined
order by mom_change_eur asc
limit 1
""",
    },
    {
        'slug': 'task2-item-pair-affinity-top',
        'name': 'Q3 Top Item Pair Affinity',
        'description': 'Top item pairs by lift and order co-occurrence.',
        'config': {
            'type': 'vertical_bar',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'category', 'reference': 'item_pair'},
                'y': [{'aggregation': 'any', 'reference': 'lift'}],
            },
            'display': {},
        },
        'sql': f"""
with latest as (
  select max(snapshot_date) as snapshot_date
  from `{dev_project}.{dev_dataset}_rpt.rpt_item_pair_affinity`
)
select
  period_month,
  concat(item_name_preferred_1, ' + ', item_name_preferred_2) as item_pair,
  orders_together,
  support,
  confidence_1_to_2,
  confidence_2_to_1,
  lift
from `{dev_project}.{dev_dataset}_rpt.rpt_item_pair_affinity`
where snapshot_date = (select snapshot_date from latest)
order by lift desc, orders_together desc
limit 200
""",
        'insight_sql': f"""
with latest as (
  select max(snapshot_date) as snapshot_date
  from `{dev_project}.{dev_dataset}_rpt.rpt_item_pair_affinity`
)
select
  'Strongest pair (by lift)' as insight_title,
  concat(item_name_preferred_1, ' + ', item_name_preferred_2) as insight_value,
  concat('lift=', cast(round(lift, 3) as string), ', orders=', cast(orders_together as string)) as insight_comment
from `{dev_project}.{dev_dataset}_rpt.rpt_item_pair_affinity`
where snapshot_date = (select snapshot_date from latest)
order by lift desc, orders_together desc
limit 1
""",
    },
    {
        'slug': 'task2-category-consumption-by-daypart',
        'name': 'Q4 Category Consumption By Daypart',
        'description': 'How each category is consumed across Berlin local dayparts.',
        'config': {
            'type': 'vertical_bar',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'category', 'reference': 'daypart'},
                'y': [{'aggregation': 'any', 'reference': 'units_sold'}],
                'groupBy': [{'reference': 'item_category'}],
                'stack': 'stack',
            },
            'display': {},
        },
        'sql': f"""
with base as (
  select
    case
      when extract(hour from datetime(order_ts_utc, 'Europe/Berlin')) between 6 and 10 then 'Morning (06-10)'
      when extract(hour from datetime(order_ts_utc, 'Europe/Berlin')) between 11 and 14 then 'Lunch (11-14)'
      when extract(hour from datetime(order_ts_utc, 'Europe/Berlin')) between 15 and 17 then 'Afternoon (15-17)'
      when extract(hour from datetime(order_ts_utc, 'Europe/Berlin')) between 18 and 22 then 'Evening (18-22)'
      else 'Night (23-05)'
    end as daypart,
    item_category,
    units_in_order_item_row as units_sold
  from `{dev_project}.{dev_dataset}_core.fct_order_item`
)
select
  daypart,
  item_category,
  sum(units_sold) as units_sold
from base
group by 1,2
order by
  case daypart
    when 'Morning (06-10)' then 1
    when 'Lunch (11-14)' then 2
    when 'Afternoon (15-17)' then 3
    when 'Evening (18-22)' then 4
    else 5
  end,
  units_sold desc
""",
        'insight_sql': f"""
with base as (
  select
    case
      when extract(hour from datetime(order_ts_utc, 'Europe/Berlin')) between 6 and 10 then 'Morning (06-10)'
      when extract(hour from datetime(order_ts_utc, 'Europe/Berlin')) between 11 and 14 then 'Lunch (11-14)'
      when extract(hour from datetime(order_ts_utc, 'Europe/Berlin')) between 15 and 17 then 'Afternoon (15-17)'
      when extract(hour from datetime(order_ts_utc, 'Europe/Berlin')) between 18 and 22 then 'Evening (18-22)'
      else 'Night (23-05)'
    end as daypart,
    item_category,
    units_in_order_item_row as units_sold
  from `{dev_project}.{dev_dataset}_core.fct_order_item`
), agg as (
  select daypart, item_category, sum(units_sold) as units_sold
  from base
  group by 1,2
), ranked as (
  select
    daypart,
    item_category,
    units_sold,
    row_number() over(partition by daypart order by units_sold desc) as rn
  from agg
), top_signal as (
  select
    daypart,
    item_category,
    units_sold
  from ranked
  where rn = 1
  order by units_sold desc
  limit 1
)
select
  'Top daypart-category by units' as insight_title,
  (select concat(daypart, ': ', item_category) from top_signal) as insight_value,
  (select concat('units=', cast(units_sold as string), '; calc=SUM(units_in_order_item_row) by Berlin daypart + category') from top_signal) as insight_comment
""",
    },
    {
        'slug': 'task2-first-order-promo-acquisition',
        'name': 'Q5 First-Order Promo Acquisition',
        'description': 'First-order promo adoption and promo-only behavior.',
        'config': {
            'type': 'pie',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'category', 'reference': 'segment'},
                'y': [{'aggregation': 'any', 'reference': 'customers'}],
            },
            'display': {'isDonut': True},
        },
        'sql': f"""
with latest as (
  select max(snapshot_date) as snapshot_date
  from `{dev_project}.{dev_dataset}_rpt.rpt_customer_promo_behavior`
)
select 'First order had any promo' as segment, sum(cast(first_order_had_any_promo_units as int64)) as customers
from `{dev_project}.{dev_dataset}_rpt.rpt_customer_promo_behavior`
where snapshot_date = (select snapshot_date from latest)
union all
select 'First order had only promo', sum(cast(first_order_had_only_promo_units as int64)) as customers
from `{dev_project}.{dev_dataset}_rpt.rpt_customer_promo_behavior`
where snapshot_date = (select snapshot_date from latest)
union all
select 'No promo on first order', count(*) - sum(cast(first_order_had_any_promo_units as int64)) as customers
from `{dev_project}.{dev_dataset}_rpt.rpt_customer_promo_behavior`
where snapshot_date = (select snapshot_date from latest)
""",
        'insight_sql': f"""
with latest as (
  select max(snapshot_date) as snapshot_date
  from `{dev_project}.{dev_dataset}_rpt.rpt_customer_promo_behavior`
), agg as (
  select
    count(*) as customers,
    sum(cast(first_order_had_any_promo_units as int64)) as first_order_with_any_promo,
    sum(cast(first_order_had_only_promo_units as int64)) as first_order_only_promo
  from `{dev_project}.{dev_dataset}_rpt.rpt_customer_promo_behavior`
  where snapshot_date = (select snapshot_date from latest)
)
select
  'Promo-acquired first orders' as insight_title,
  concat(cast(round(safe_divide(first_order_with_any_promo, nullif(customers, 0)) * 100, 2) as string), '%') as insight_value,
  concat('Only-promo first orders: ', cast(round(safe_divide(first_order_only_promo, nullif(customers, 0)) * 100, 2) as string), '%') as insight_comment
from agg
""",
    },
]

chart_results = []
guide_meta = {
    'task2-category-monthly-growth': {
        'what_it_says': 'Shows monthly revenue trend by item category to identify growth leaders and laggards.',
        'main_metric': 'Revenue (EUR) by category and month.',
        'metric_calc': 'SUM(revenue_eur) grouped by date_trunc(period_month, month) and item_category from rpt_category_daily latest snapshot.',
        'how_to_use': 'Pick categories with consistently rising curves; investigate categories with flattening/declining trend.',
    },
    'task2-star-products-by-category': {
        'what_it_says': 'Ranks products by revenue within each category to identify star SKUs.',
        'main_metric': 'Product revenue (EUR).',
        'metric_calc': 'SUM(order_item_row_final_amount_gross_eur) grouped by item_category and item_name_preferred, sorted DESC.',
        'how_to_use': 'Use top products for merchandising and stock planning; watch concentration risk if one SKU dominates.',
    },
    'task2-declining-categories-mom': {
        'what_it_says': 'Highlights categories with negative month-over-month revenue change.',
        'main_metric': 'MoM revenue delta (EUR).',
        'metric_calc': 'Current month category revenue minus previous month category revenue using LAG over monthly series.',
        'how_to_use': 'Prioritize categories with largest negative delta for root-cause analysis (price, assortment, promo, supply).',
    },
    'task2-item-pair-affinity-top': {
        'what_it_says': 'Shows product pairs bought together frequently and above-random expectation.',
        'main_metric': 'Lift and orders_together.',
        'metric_calc': 'Lift = support(pair) / (support(item1) * support(item2)); filtered to top pairs by lift and co-orders.',
        'how_to_use': 'Use high-lift pairs for bundle design, recommendations, and cross-sell placements.',
    },
    'task2-category-consumption-by-daypart': {
        'what_it_says': 'Shows when customers consume each category across Berlin-local dayparts.',
        'main_metric': 'Units sold by daypart and category.',
        'metric_calc': 'Convert order_ts_utc to Europe/Berlin hour, map hour to daypart bucket, then SUM(units_in_order_item_row) by daypart + item_category.',
        'how_to_use': 'Use for daypart assortment, promo timing, and inventory planning by category.',
    },
    'task2-first-order-promo-acquisition': {
        'what_it_says': 'Breaks first orders into any-promo, only-promo, and no-promo segments.',
        'main_metric': 'Customer count by first-order promo segment.',
        'metric_calc': 'Counts from rpt_customer_promo_behavior latest snapshot using first_order_had_any_promo_units and first_order_had_only_promo_units.',
        'how_to_use': 'Track promo-led acquisition quality, not just volume; compare with repeat behavior downstream.',
    },
}

for q in queries:
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
    chart_results.append({
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
    })
    enforce_table_columns(saved_sql_uuid, q['config'])
    # Publish companion insight charts so they can be added manually in UI
    # without forcing dashboard layout changes.
    insight_uuid, _ = upsert_sql_chart(
        session=session,
        base_url=base_url,
        project_uuid=project_uuid,
        space_uuid=space_uuid,
        slug=f"{q['slug']}-insight",
        name=f"Insight: {q['name']}",
        description=f"Auto-updated insight for {q['name']}",
        sql=q['insight_sql'],
        config={
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'insight_title': {'visible': True},
                'insight_value': {'visible': True},
                'insight_comment': {'visible': True},
            },
        },
    )
    enforce_table_columns(insight_uuid, {
        'type': 'table',
        'metadata': {'version': 1},
        'columns': {
            'insight_title': {'visible': True},
            'insight_value': {'visible': True},
            'insight_comment': {'visible': True},
        },
    })

dashboard_uuid = create_dashboard(
    session=session,
    base_url=base_url,
    project_uuid=project_uuid,
    dashboard_name='Task 2 - Business Visual Pack',
    description='Auto-generated dashboard for assignment Task 2 business questions.',
    charts=chart_results,
)

print(f"Authenticated as: {user['email']}")
print(f"Project UUID: {project_uuid}")
print(f"Space UUID: {space_uuid}")
print(f"Dashboard UUID: {dashboard_uuid}")
print(f"Open dashboard: http://localhost:8080/projects/{project_uuid}/dashboards/{dashboard_uuid}")
PY
