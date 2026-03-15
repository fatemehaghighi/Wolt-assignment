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
        delete_resp = session.delete(
            f"{base_url}/api/v1/dashboards/{existing['uuid']}",
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
        'slug': 'task2-q1-dd-kpi-control-primary',
        'name': 'Q1 KPI Selector (Category x Month)',
        'description': 'Select KPI via dashboard filter kpi_name and trend by category-month.',
        'config': {
            'type': 'line',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'time', 'reference': 'period_month'},
                'y': [{'aggregation': 'any', 'reference': 'kpi_value'}],
                'groupBy': [{'reference': 'item_category'}],
            },
            'display': {},
        },
        'sql': base_sql + """
, cat_month as (
  select
    date_trunc(order_date, month) as period_month,
    item_category,
    sum(revenue_eur) as revenue_eur,
    count(distinct order_sk) as orders,
    count(distinct customer_sk) as customers,
    sum(units_sold) as units_sold,
    safe_divide(sum(discount_eur), nullif(sum(base_revenue_eur), 0)) as discount_rate,
    safe_divide(sum(revenue_eur), nullif(sum(units_sold), 0)) as weighted_avg_selling_price_eur,
    safe_divide(sum(revenue_eur), nullif(count(distinct order_sk), 0)) as revenue_per_order_eur
  from mapped
  group by 1, 2
),
kpi_long as (
  select period_month, item_category, 'Revenue EUR' as kpi_name, cast(revenue_eur as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Orders' as kpi_name, cast(orders as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Customers' as kpi_name, cast(customers as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Units Sold' as kpi_name, cast(units_sold as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Discount Rate' as kpi_name, cast(discount_rate as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Weighted Avg Selling Price EUR' as kpi_name, cast(weighted_avg_selling_price_eur as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Revenue Per Order EUR' as kpi_name, cast(revenue_per_order_eur as float64) as kpi_value from cat_month
)
select
  period_month,
  item_category,
  kpi_name,
  kpi_value
from kpi_long
order by period_month, item_category, kpi_name
""",
        'guide_md': guide(
            'Interactive KPI trend by category and month.',
            'kpi_value controlled by kpi_name.',
            'One long-format dataset with KPI switch values (Revenue, Orders, Customers, Units, Discount Rate, ASP, Revenue per Order).',
            'Use Add filter -> kpi_name, select one KPI to switch the same chart.',
        ),
    },
    {
        'slug': 'task2-q1-dd-raw-core-scale',
        'name': 'Q1 Raw Category Performance - Core Scale',
        'description': 'Raw-file category ranking by scale and penetration.',
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'item_category': {'visible': True},
                'revenue_eur': {'visible': True},
                'revenue_share': {'visible': True},
                'orders': {'visible': True},
                'order_penetration': {'visible': True},
                'customers': {'visible': True},
                'customer_penetration': {'visible': True},
                'units_sold': {'visible': True},
            },
        },
        'sql': base_sql + """
, totals as (
  select
    sum(revenue_eur) as total_revenue,
    count(distinct order_sk) as total_orders,
    count(distinct customer_sk) as total_customers
  from mapped
)
select
  item_category,
  sum(revenue_eur) as revenue_eur,
  safe_divide(sum(revenue_eur), (select total_revenue from totals)) as revenue_share,
  count(distinct order_sk) as orders,
  safe_divide(count(distinct order_sk), (select total_orders from totals)) as order_penetration,
  count(distinct customer_sk) as customers,
  safe_divide(count(distinct customer_sk), (select total_customers from totals)) as customer_penetration,
  sum(units_sold) as units_sold
from mapped
group by 1
order by revenue_eur desc
""",
        'guide_md': guide(
            'Raw category winner view before classification correction.',
            'Revenue share, order penetration, customer penetration, units.',
            'Penetration uses total distinct orders/customers as denominator.',
            'Use this to state raw leaderboard, then compare with adjusted view.',
        ),
    },
    {
        'slug': 'task2-q1-dd-raw-quality-concentration',
        'name': 'Q1 Raw Category Quality + Concentration',
        'description': 'Category efficiency and risk metrics (raw mapping).',
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'item_category': {'visible': True},
                'asp_eur': {'visible': True},
                'discount_rate': {'visible': True},
                'revenue_per_order_eur': {'visible': True},
                'units_per_order': {'visible': True},
                'orders_per_customer': {'visible': True},
                'repeat_customer_rate': {'visible': True},
                'sku_count': {'visible': True},
                'top_sku_share': {'visible': True},
                'top_3_share': {'visible': True},
            },
        },
        'sql': base_sql + """
, cat_base as (
  select
    item_category,
    sum(revenue_eur) as revenue_eur,
    sum(base_revenue_eur) as base_revenue_eur,
    sum(discount_eur) as discount_eur,
    sum(units_sold) as units_sold,
    count(distinct order_sk) as orders,
    count(distinct customer_sk) as customers
  from mapped
  group by 1
), cust_cat as (
  select
    item_category,
    customer_sk,
    count(distinct order_sk) as orders_per_customer
  from mapped
  group by 1,2
), cust_roll as (
  select
    item_category,
    avg(orders_per_customer) as orders_per_customer,
    safe_divide(sum(case when orders_per_customer > 1 then 1 else 0 end), count(*)) as repeat_customer_rate
  from cust_cat
  group by 1
), sku as (
  select
    item_category,
    product_name,
    sum(revenue_eur) as sku_revenue
  from mapped
  group by 1,2
), sku_rank as (
  select
    *,
    row_number() over(partition by item_category order by sku_revenue desc) as sku_rank
  from sku
)
select
  c.item_category,
  safe_divide(c.revenue_eur, nullif(c.units_sold, 0)) as asp_eur,
  safe_divide(c.discount_eur, nullif(c.base_revenue_eur, 0)) as discount_rate,
  safe_divide(c.revenue_eur, nullif(c.orders, 0)) as revenue_per_order_eur,
  safe_divide(c.units_sold, nullif(c.orders, 0)) as units_per_order,
  cr.orders_per_customer,
  cr.repeat_customer_rate,
  count(distinct sr.product_name) as sku_count,
  safe_divide(sum(case when sr.sku_rank = 1 then sr.sku_revenue else 0 end), nullif(c.revenue_eur, 0)) as top_sku_share,
  safe_divide(sum(case when sr.sku_rank <= 3 then sr.sku_revenue else 0 end), nullif(c.revenue_eur, 0)) as top_3_share
from cat_base c
left join cust_roll cr on c.item_category = cr.item_category
left join sku_rank sr on c.item_category = sr.item_category
group by 1,2,3,4,5,6,7,c.revenue_eur
order by c.revenue_eur desc
""",
        'guide_md': guide(
            'Quality and concentration diagnostics for each raw category.',
            'ASP, discount rate, repeat rate, top-SKU concentration.',
            'Top SKU share and Top3 share are revenue concentration metrics.',
            'Use to separate broad-based wins from fragile single-SKU categories.',
        ),
    },
    {
        'slug': 'task2-q1-dd-adjusted-category-view',
        'name': 'Q1 Adjusted Category View (Tonys -> Chocolate)',
        'description': 'Business steering view after category mapping correction.',
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
, totals as (
  select
    sum(revenue_eur) as total_revenue,
    count(distinct order_sk) as total_orders,
    count(distinct customer_sk) as total_customers
  from mapped
)
select
  adjusted_category,
  sum(revenue_eur) as revenue_eur,
  safe_divide(sum(revenue_eur), (select total_revenue from totals)) as revenue_share,
  count(distinct order_sk) as orders,
  safe_divide(count(distinct order_sk), (select total_orders from totals)) as order_penetration,
  count(distinct customer_sk) as customers,
  safe_divide(count(distinct customer_sk), (select total_customers from totals)) as customer_penetration,
  sum(units_sold) as units_sold,
  safe_divide(sum(revenue_eur), nullif(sum(units_sold),0)) as asp_eur,
  safe_divide(sum(discount_eur), nullif(sum(base_revenue_eur),0)) as discount_rate
from mapped
group by 1
order by revenue_eur desc
""",
        'guide_md': guide(
            'Adjusted ranking after fixing known category governance issue.',
            'Adjusted category revenue and share.',
            'Tonys brownie SKU is normalized to Chocolate for business-steering comparability.',
            'Use this as the final category ranking in stakeholder narrative.',
        ),
    },
    {
        'slug': 'task2-q1-dd-monthly-trends-raw',
        'name': 'Q1 Monthly Revenue Trends (Raw Categories)',
        'description': 'Monthly trend lines by raw category.',
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
        'sql': base_sql + """
select
  date_trunc(order_date, month) as period_month,
  item_category,
  sum(revenue_eur) as revenue_eur,
  sum(units_sold) as units_sold
from mapped
group by 1,2
order by 1,3 desc
""",
        'guide_md': guide(
            'Monthly pattern and momentum by raw categories.',
            'Monthly category revenue.',
            'Revenue is aggregated from final paid line amounts by category-month.',
            'Use with adjusted view to separate true demand from remapping effects.',
        ),
    },
    {
        'slug': 'task2-q1-dd-kpi-control-by-category-month',
        'name': 'Q1 KPI Control (Pick KPI -> Category x Month)',
        'description': 'Single trend chart controlled by KPI selector (kpi_name filter).',
        'config': {
            'type': 'line',
            'metadata': {'version': 1},
            'fieldConfig': {
                'x': {'type': 'time', 'reference': 'period_month'},
                'y': [{'aggregation': 'any', 'reference': 'kpi_value'}],
                'groupBy': [{'reference': 'item_category'}],
            },
            'display': {},
        },
        'sql': base_sql + """
, cat_month as (
  select
    date_trunc(order_date, month) as period_month,
    item_category,
    sum(revenue_eur) as revenue_eur,
    count(distinct order_sk) as orders,
    count(distinct customer_sk) as customers,
    sum(units_sold) as units_sold,
    safe_divide(sum(discount_eur), nullif(sum(base_revenue_eur), 0)) as discount_rate,
    safe_divide(sum(revenue_eur), nullif(sum(units_sold), 0)) as weighted_avg_selling_price_eur,
    safe_divide(sum(revenue_eur), nullif(count(distinct order_sk), 0)) as revenue_per_order_eur
  from mapped
  group by 1, 2
),
kpi_long as (
  select period_month, item_category, 'Revenue EUR' as kpi_name, cast(revenue_eur as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Orders' as kpi_name, cast(orders as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Customers' as kpi_name, cast(customers as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Units Sold' as kpi_name, cast(units_sold as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Discount Rate' as kpi_name, cast(discount_rate as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Weighted Avg Selling Price EUR' as kpi_name, cast(weighted_avg_selling_price_eur as float64) as kpi_value from cat_month
  union all
  select period_month, item_category, 'Revenue Per Order EUR' as kpi_name, cast(revenue_per_order_eur as float64) as kpi_value from cat_month
)
select
  period_month,
  item_category,
  kpi_name,
  kpi_value
from kpi_long
order by period_month, item_category, kpi_name
""",
        'guide_md': guide(
            'One reusable chart for multiple KPIs by month and category.',
            'kpi_value (filtered by kpi_name).',
            'KPIs are normalized into a long format: one row per category-month-KPI.',
            'Use dashboard top filter on kpi_name (select one KPI) to switch chart instantly.',
        ),
    },
    {
        'slug': 'task2-q1-dd-cross-sell-attach',
        'name': 'Q1 Cross-sell Attach Rates',
        'description': 'Attach rates to identify basket-building categories.',
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
  select distinct order_sk, item_category
  from mapped
), pairs as (
  select
    a.item_category as base_category,
    b.item_category as attached_category,
    count(distinct a.order_sk) as attached_orders
  from order_cats a
  join order_cats b
    on a.order_sk = b.order_sk
   and a.item_category <> b.item_category
  group by 1,2
), base as (
  select item_category as base_category, count(distinct order_sk) as base_orders
  from order_cats
  group by 1
)
select
  p.base_category,
  p.attached_category,
  b.base_orders,
  p.attached_orders,
  safe_divide(p.attached_orders, nullif(b.base_orders,0)) as attach_rate
from pairs p
join base b using(base_category)
order by attach_rate desc, attached_orders desc
limit 50
""",
        'guide_md': guide(
            'Shows which categories are strongest basket companions.',
            'Attach rate of category pairs.',
            'Attach rate = orders containing both A and B / orders containing A.',
            'Use for bundle strategy and cross-sell placements.',
        ),
    },
    {
        'slug': 'task2-q1-dd-star-products-top10',
        'name': 'Q1 Top 10 Star Products',
        'description': 'Hero products ranked by commercial contribution.',
        'config': {
            'type': 'table',
            'metadata': {'version': 1},
            'columns': {
                'product_name': {'visible': True},
                'category_mapping_in_file': {'visible': True},
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
  select sum(revenue_eur) as total_revenue
  from mapped
), stars as (
  select
    product_name,
    string_agg(distinct item_category, ', ' order by item_category) as category_mapping_in_file,
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
  category_mapping_in_file,
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
            'Ranks star SKUs with concentration context.',
            'Revenue share, order reach, customer reach.',
            'Top products ranked by SUM(revenue_eur) across 2023.',
            'Use to monitor hero dependency risk and prioritize portfolio actions.',
        ),
    },
    {
        'slug': 'task2-q1-dd-executive-summary',
        'name': 'Q1 Executive Summary (Auto Insights)',
        'description': 'Auto-updated decision headlines from latest data.',
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
), adj_rank as (
  select adjusted_category as category, sum(revenue_eur) as revenue_eur
  from mapped
  group by 1
), stars as (
  select product_name, sum(revenue_eur) as revenue_eur,
         row_number() over(order by sum(revenue_eur) desc) as rn
  from mapped
  group by 1
), dipping_attach as (
  select
    safe_divide(
      sum(case when has_dipping = 1 and has_crisps = 1 then 1 else 0 end),
      nullif(sum(case when has_dipping = 1 then 1 else 0 end), 0)
    ) as attach_rate
  from (
    select
      order_sk,
      max(case when item_category = 'Cheese Crackers, Breadsticks & Dipping' then 1 else 0 end) as has_dipping,
      max(case when item_category = 'Crisp & Snacks' then 1 else 0 end) as has_crisps
    from mapped
    group by 1
  )
)
select
  'Raw-file winner category' as insight_title,
  (select category from raw_rank order by revenue_eur desc limit 1) as insight_value,
  concat('Revenue EUR: ', cast(round((select revenue_eur from raw_rank order by revenue_eur desc limit 1), 2) as string)) as insight_comment
union all
select
  'Adjusted winner category',
  (select category from adj_rank order by revenue_eur desc limit 1),
  concat('Revenue EUR: ', cast(round((select revenue_eur from adj_rank order by revenue_eur desc limit 1), 2) as string))
union all
select
  'Top 2 SKU revenue concentration',
  cast(round(safe_divide((select sum(revenue_eur) from stars where rn <= 2), (select sum(revenue_eur) from mapped)) * 100, 2) as string),
  '% of total revenue'
union all
select
  'Dipping -> Crisp attach rate',
  cast(round((select attach_rate from dipping_attach) * 100, 2) as string),
  '% of dipping orders also include Crisp & Snacks'
""",
        'guide_md': guide(
            'Auto-generated final decision headlines for Q1.',
            'Raw winner vs adjusted winner, concentration, attach rate.',
            'Derived directly from underlying fact tables and category mapping logic.',
            'Use this as executive summary slide/table; validate with detailed blocks above.',
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
    dashboard_name='Task 2 - Q1 Category Deep Dive (Business Analysis)',
    description='Q1 deep-dive dashboard based on order-item commercial performance analysis with raw vs adjusted category views.',
    charts=chart_results,
)

print(json.dumps({
    'project_uuid': project_uuid,
    'space_uuid': space_uuid,
    'dashboard_uuid': dashboard_uuid,
    'url': f'http://localhost:18080/projects/{project_uuid}/dashboards/{dashboard_uuid}/view'
}, indent=2))
PY
