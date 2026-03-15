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
        raise RuntimeError(f"Lightdash project '{project_name}' not found.")
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


def normalize_table_config(config: dict) -> dict:
    if config.get('type') != 'table':
        return config
    columns = config.get('columns', {})
    normalized = {}
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
        if patch.status_code >= 400:
            print(f"PATCH failed for slug={slug}: {patch.status_code} {patch.text}")
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
    if create.status_code >= 400:
        print(f"CREATE failed for slug={slug}: {create.status_code} {create.text}")
    create.raise_for_status()
    return create.json()['results']['savedSqlUuid']


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
        session.delete(f"{base_url}/api/v1/projects/{project_uuid}/dashboards/{existing['uuid']}", timeout=20).raise_for_status()

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
        'tabs': [{'uuid': tab_uuid, 'name': 'Q1 Deep Dive', 'order': 0}],
        'tiles': tiles,
    }
    created = session.post(f'{base_url}/api/v1/projects/{project_uuid}/dashboards', json=payload, timeout=30)
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
        'docker', 'exec', 'lightdash_postgres', 'psql', '-U', 'lightdash', '-d', 'lightdash', '-Atc',
        'select sid from sessions order by expired desc limit 1;'
    ],
    text=True,
).strip()
if not sid:
    raise SystemExit('No active Lightdash session found. Login first.')

session = requests.Session()
session.headers.update({'Cookie': f"connect.sid={signed_cookie(secret, sid)}", 'Content-Type': 'application/json'})
base_url = 'http://localhost:18080'

project_name = os.environ.get('LIGHTDASH_PROJECT_NAME') or env.get('LIGHTDASH_PROJECT_NAME', 'Wolt Assignment Dev Semantic')
project_uuid = ensure_project(session, base_url, project_name)
space_uuid = ensure_space(session, base_url, project_uuid, 'Task 2 Visuals')

dev_project = env['DBT_BQ_DEV_PROJECT']
dev_dataset = env['DBT_BQ_DEV_DATASET']

base_sql = f"""
with base as (
  select
    order_date,
    order_sk,
    customer_sk,
    item_key,
    coalesce(item_name_preferred, item_name_en, item_name_de, 'Unknown Item') as product_name,
    coalesce(item_category, 'Unknown') as item_category,
    units_in_order_item_row as units_sold,
    order_item_row_final_amount_gross_eur as revenue_eur,
    order_item_row_base_amount_gross_eur as base_revenue_eur,
    order_item_row_discount_amount_gross_eur as discount_eur
  from `{dev_project}.{dev_dataset}_core.fct_order_item`
  where order_date between date '2023-01-01' and date '2023-12-31'
),
mapped as (
  select
    *,
    case
      when lower(product_name) like '%tony%chocolonely%dark milk%brownie%180%' then 'Chocolate'
      else item_category
    end as adjusted_category
  from base
)
"""

charts_config = [
    {
        'slug': 'task2-q1-mockup-overview-kpis',
        'name': 'Q1 Overview KPIs',
        'description': 'Order-item level scope and totals for 2023.',
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'order_item_rows': {'visible': True},
                'orders': {'visible': True},
                'customers': {'visible': True},
                'products': {'visible': True},
                'categories': {'visible': True},
                'units_sold': {'visible': True},
                'final_revenue_eur': {'visible': True},
                'base_revenue_eur': {'visible': True},
                'discount_eur': {'visible': True},
                'discount_rate': {'visible': True},
            },
        },
        'sql': base_sql + """
select
  count(*) as order_item_rows,
  count(distinct order_sk) as orders,
  count(distinct customer_sk) as customers,
  count(distinct item_key) as products,
  count(distinct item_category) as categories,
  sum(units_sold) as units_sold,
  round(sum(revenue_eur), 2) as final_revenue_eur,
  round(sum(base_revenue_eur), 2) as base_revenue_eur,
  round(sum(discount_eur), 2) as discount_eur,
  round(safe_divide(sum(discount_eur), nullif(sum(base_revenue_eur), 0)), 4) as discount_rate
from mapped
""",
        'guide_md': guide(
            'Base scope and size used in all Q1 charts.',
            'Final revenue, units sold, orders, customers.',
            'Direct SUM/COUNT DISTINCT over 2023 fact_order_item rows.',
            'Validate scope first before interpreting category ranking.',
        ),
    },
    {
        'slug': 'task2-q1-mockup-raw-adjusted-scorecard',
        'name': 'Q1 Raw vs Adjusted Category Scorecard',
        'description': 'Compares raw file category mapping to adjusted mapping.',
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'view_type': {'visible': True},
                'category': {'visible': True},
                'revenue_eur': {'visible': True},
                'revenue_share': {'visible': True},
                'orders': {'visible': True},
                'order_penetration': {'visible': True},
                'customers': {'visible': True},
                'customer_penetration': {'visible': True},
                'units_sold': {'visible': True},
                'asp_eur': {'visible': True},
                'discount_rate': {'visible': True},
            },
        },
        'sql': base_sql + """
, totals as (
  select
    count(distinct order_sk) as total_orders,
    count(distinct customer_sk) as total_customers,
    sum(revenue_eur) as total_revenue
  from mapped
),
raw_view as (
  select
    'raw' as view_type,
    item_category as category,
    sum(revenue_eur) as revenue_eur,
    safe_divide(sum(revenue_eur), (select total_revenue from totals)) as revenue_share,
    count(distinct order_sk) as orders,
    safe_divide(count(distinct order_sk), (select total_orders from totals)) as order_penetration,
    count(distinct customer_sk) as customers,
    safe_divide(count(distinct customer_sk), (select total_customers from totals)) as customer_penetration,
    sum(units_sold) as units_sold,
    safe_divide(sum(revenue_eur), nullif(sum(units_sold), 0)) as asp_eur,
    safe_divide(sum(discount_eur), nullif(sum(base_revenue_eur), 0)) as discount_rate
  from mapped
  group by 1,2
),
adjusted_view as (
  select
    'adjusted' as view_type,
    adjusted_category as category,
    sum(revenue_eur) as revenue_eur,
    safe_divide(sum(revenue_eur), (select total_revenue from totals)) as revenue_share,
    count(distinct order_sk) as orders,
    safe_divide(count(distinct order_sk), (select total_orders from totals)) as order_penetration,
    count(distinct customer_sk) as customers,
    safe_divide(count(distinct customer_sk), (select total_customers from totals)) as customer_penetration,
    sum(units_sold) as units_sold,
    safe_divide(sum(revenue_eur), nullif(sum(units_sold), 0)) as asp_eur,
    safe_divide(sum(discount_eur), nullif(sum(base_revenue_eur), 0)) as discount_rate
  from mapped
  group by 1,2
)
select * from raw_view
union all
select * from adjusted_view
order by view_type, revenue_eur desc
""",
        'guide_md': guide(
            "Shows the category reclassification caveat impact (Tony's brownie).",
            'Revenue share and penetration by category in raw vs adjusted views.',
            "Adjusted view remaps Tony's brownie SKU to Chocolate.",
            'Use adjusted view for steering decisions; keep raw view for data-governance transparency.',
        ),
    },
    {
        'slug': 'task2-q1-mockup-revenue-share-raw',
        'name': 'Q1 Revenue Distribution (Raw View)',
        'description': 'Revenue share by category using raw category mapping.',
        'config': {
            'type': 'pie',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'category', 'reference': 'category'},
                'y': [{'aggregation': 'any', 'reference': 'revenue_eur'}],
            },
            'display': {'isDonut': True},
        },
        'sql': base_sql + """
select
  item_category as category,
  sum(revenue_eur) as revenue_eur
from mapped
group by 1
order by revenue_eur desc
""",
        'guide_md': guide(
            'Category revenue mix exactly as stored in source mapping.',
            'Revenue EUR share by category.',
            'Revenue EUR = SUM(order_item_row_final_amount_gross_eur).',
            'Quickly identify major revenue buckets before quality diagnostics.',
        ),
    },
    {
        'slug': 'task2-q1-mockup-revenue-share-adjusted',
        'name': 'Q1 Revenue Ranking (Adjusted View)',
        'description': 'Revenue by category after Tony’s mapping normalization.',
        'config': {
            'type': 'vertical_bar',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'category', 'reference': 'adjusted_category'},
                'y': [{'aggregation': 'any', 'reference': 'revenue_eur'}],
            },
            'display': {},
        },
        'sql': base_sql + """
select
  adjusted_category,
  sum(revenue_eur) as revenue_eur
from mapped
group by 1
order by revenue_eur desc
""",
        'guide_md': guide(
            'Business-steering category ranking with normalized mapping.',
            'Adjusted category revenue EUR.',
            "Tony's brownie rows are mapped to Chocolate before aggregation.",
            'Use this for final ranking communication to business.',
        ),
    },
    {
        'slug': 'task2-q1-mockup-monthly-trend',
        'name': 'Q1 Monthly Revenue Trends (Raw)',
        'description': 'Monthly revenue trend by raw category.',
        'config': {
            'type': 'line',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'time', 'reference': 'period_month'},
                'y': [{'aggregation': 'any', 'reference': 'revenue_eur'}],
                'groupBy': [{'reference': 'category'}],
            },
            'display': {},
        },
        'sql': base_sql + """
select
  date_trunc(order_date, month) as period_month,
  item_category as category,
  sum(revenue_eur) as revenue_eur
from mapped
group by 1,2
order by 1,3 desc
""",
        'guide_md': guide(
            'Shows seasonality and trend slope by raw category.',
            'Monthly category revenue.',
            'Grouped by DATE_TRUNC(order_date, MONTH) and category.',
            'Compare slope, volatility, and turning points across categories.',
        ),
    },
    {
        'slug': 'task2-q1-mockup-mom-growth',
        'name': 'Q1 Month-over-Month Revenue Growth (Raw)',
        'description': 'MoM revenue growth ratio by raw category.',
        'config': {
            'type': 'line',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'time', 'reference': 'period_month'},
                'y': [{'aggregation': 'any', 'reference': 'mom_growth_ratio'}],
                'groupBy': [{'reference': 'category'}],
            },
            'display': {},
        },
        'sql': base_sql + """
, monthly as (
  select
    date_trunc(order_date, month) as period_month,
    item_category as category,
    sum(revenue_eur) as revenue_eur
  from mapped
  group by 1,2
)
select
  period_month,
  category,
  safe_divide(
    revenue_eur - lag(revenue_eur) over(partition by category order by period_month),
    nullif(lag(revenue_eur) over(partition by category order by period_month), 0)
  ) as mom_growth_ratio
from monthly
order by period_month, category
""",
        'guide_md': guide(
            'Growth acceleration/deceleration per category, month to month.',
            'MoM revenue growth ratio.',
            'MoM = (current_month_revenue - previous_month_revenue) / previous_month_revenue.',
            'Use with trend chart to separate structural growth from noise.',
        ),
    },
    {
        'slug': 'task2-q1-mockup-discount-concentration',
        'name': 'Q1 Discount Dependency and Concentration',
        'description': 'Discount rate and top SKU concentration by category.',
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'category': {'visible': True},
                'discount_rate': {'visible': True},
                'top_sku_share': {'visible': True},
                'top_3_share': {'visible': True},
                'sku_count': {'visible': True},
            },
        },
        'sql': base_sql + """
, sku as (
  select
    item_category as category,
    product_name,
    sum(revenue_eur) as sku_revenue
  from mapped
  group by 1,2
),
ranked as (
  select
    *,
    row_number() over(partition by category order by sku_revenue desc) as sku_rank
  from sku
),
cat as (
  select
    item_category as category,
    safe_divide(sum(discount_eur), nullif(sum(base_revenue_eur), 0)) as discount_rate,
    sum(revenue_eur) as cat_revenue,
    count(distinct product_name) as sku_count
  from mapped
  group by 1
)
select
  c.category,
  c.discount_rate,
  safe_divide(sum(case when r.sku_rank = 1 then r.sku_revenue else 0 end), nullif(c.cat_revenue, 0)) as top_sku_share,
  safe_divide(sum(case when r.sku_rank <= 3 then r.sku_revenue else 0 end), nullif(c.cat_revenue, 0)) as top_3_share,
  c.sku_count
from cat c
left join ranked r using(category)
group by 1,2,5,c.cat_revenue
order by c.cat_revenue desc
""",
        'guide_md': guide(
            'Highlights whether category growth is broad or concentrated and promo-assisted.',
            'Discount rate, top SKU share, top-3 share.',
            'Discount rate = discount/base; concentration = top SKU revenue shares.',
            'High concentration + high discount dependency indicates fragile growth quality.',
        ),
    },
    {
        'slug': 'task2-q1-mockup-cross-sell',
        'name': 'Q1 Cross-sell Attach Rates',
        'description': 'Category pair attach rates (A with B / A).',
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'base_category': {'visible': True},
                'attached_category': {'visible': True},
                'base_orders': {'visible': True},
                'attached_orders': {'visible': True},
                'attach_rate': {'visible': True},
            },
        },
        'sql': base_sql + """
, order_cats as (
  select distinct order_sk, item_category as category
  from mapped
),
pairs as (
  select
    a.category as base_category,
    b.category as attached_category,
    count(distinct a.order_sk) as attached_orders
  from order_cats a
  join order_cats b
    on a.order_sk = b.order_sk
   and a.category <> b.category
  group by 1,2
),
base as (
  select category as base_category, count(distinct order_sk) as base_orders
  from order_cats
  group by 1
)
select
  p.base_category,
  p.attached_category,
  b.base_orders,
  p.attached_orders,
  safe_divide(p.attached_orders, nullif(b.base_orders, 0)) as attach_rate
from pairs p
join base b using(base_category)
order by attach_rate desc, p.attached_orders desc
limit 50
""",
        'guide_md': guide(
            'Which categories are basket builders vs followers.',
            'Attach rate between category pairs.',
            'Attach rate = orders containing both A and B / orders containing A.',
            'Use top attach pairs for bundles and recommendation slots.',
        ),
    },
    {
        'slug': 'task2-q1-mockup-star-products',
        'name': 'Q1 Top 10 Star Products',
        'description': 'Top products by commercial contribution.',
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'product_name': {'visible': True},
                'category_in_file': {'visible': True},
                'revenue_eur': {'visible': True},
                'revenue_share': {'visible': True},
                'orders': {'visible': True},
                'customers': {'visible': True},
                'asp_eur': {'visible': True},
                'discount_rate': {'visible': True},
            },
        },
        'sql': base_sql + """
, totals as (
  select sum(revenue_eur) as total_revenue from mapped
),
stars as (
  select
    product_name,
    string_agg(distinct item_category, ', ' order by item_category) as category_in_file,
    sum(revenue_eur) as revenue_eur,
    count(distinct order_sk) as orders,
    count(distinct customer_sk) as customers,
    safe_divide(sum(revenue_eur), nullif(sum(units_sold), 0)) as asp_eur,
    safe_divide(sum(discount_eur), nullif(sum(base_revenue_eur), 0)) as discount_rate
  from mapped
  group by 1
)
select
  product_name,
  category_in_file,
  revenue_eur,
  safe_divide(revenue_eur, (select total_revenue from totals)) as revenue_share,
  orders,
  customers,
  asp_eur,
  discount_rate
from stars
order by revenue_eur desc
limit 10
""",
        'guide_md': guide(
            'Ranks hero SKUs by commercial contribution.',
            'Revenue share, order reach, customer reach.',
            'Top-10 by product revenue with category mapping shown.',
            'Use to manage hero-SKU dependence and portfolio risk.',
        ),
    },
    {
        'slug': 'task2-q1-mockup-executive-summary',
        'name': 'Q1 Executive Summary (Auto)',
        'description': 'Auto-updated summary lines from data.',
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'insight_title': {'visible': True},
                'insight_value': {'visible': True},
                'insight_comment': {'visible': True},
            },
        },
        'sql': base_sql + """
, raw_rank as (
  select item_category as category, sum(revenue_eur) as revenue_eur
  from mapped
  group by 1
),
adj_rank as (
  select adjusted_category as category, sum(revenue_eur) as revenue_eur
  from mapped
  group by 1
),
stars as (
  select
    product_name,
    sum(revenue_eur) as revenue_eur,
    row_number() over(order by sum(revenue_eur) desc) as rn
  from mapped
  group by 1
),
pair as (
  select
    a.order_sk,
    max(case when a.item_category = 'Cheese Crackers, Breadsticks & Dipping' then 1 else 0 end) as has_dipping,
    max(case when a.item_category = 'Crisp & Snacks' then 1 else 0 end) as has_crisps
  from mapped a
  group by 1
)
select 'Raw top category' as insight_title,
       (select category from raw_rank order by revenue_eur desc limit 1) as insight_value,
       concat('revenue_eur=', cast(round((select revenue_eur from raw_rank order by revenue_eur desc limit 1), 2) as string)) as insight_comment
union all
select 'Adjusted top category',
       (select category from adj_rank order by revenue_eur desc limit 1),
       concat('revenue_eur=', cast(round((select revenue_eur from adj_rank order by revenue_eur desc limit 1), 2) as string))
union all
select 'Top 2 SKU concentration',
       cast(round(safe_divide((select sum(revenue_eur) from stars where rn <= 2), (select sum(revenue_eur) from mapped)) * 100, 2) as string),
       '% of total revenue from top 2 products'
union all
select 'Dipping -> Crisps attach rate',
       cast(round(safe_divide(sum(case when has_dipping = 1 and has_crisps = 1 then 1 else 0 end), nullif(sum(case when has_dipping = 1 then 1 else 0 end), 0)) * 100, 2) as string),
       '% of dipping orders that also include Crisp & Snacks'
from pair
""",
        'guide_md': guide(
            'Auto narrative block that refreshes with latest model data.',
            'Top category raw vs adjusted, top-2 SKU concentration, attach rate.',
            'Derived directly from fact_order_item aggregates; no manual text edits.',
            'Use as header insight before drilling into supporting charts.',
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
    dashboard_name='Task 2 - Q1 Category Deep Dive (Mockup)',
    description='Mockup-aligned Q1 dashboard built on fct_order_item with raw vs adjusted category views.',
    charts=chart_results,
)

print(json.dumps({
    'project_uuid': project_uuid,
    'space_uuid': space_uuid,
    'dashboard_uuid': dashboard_uuid,
    'url': f'http://localhost:18080/projects/{project_uuid}/dashboards/{dashboard_uuid}/view'
}, indent=2))
PY
