{{
    config(
        materialized='table'
    )
}}

with base as (
    select
        order_sk,
        customer_sk,
        coalesce(item_name_preferred, item_name_en, item_name_de) as product_name,
        item_category as category,
        cast(units_in_order_item_row as numeric) as units,
        cast(order_item_row_final_amount_gross_eur as numeric) as revenue_eur,
        cast(order_item_row_base_amount_gross_eur as numeric) as base_revenue_eur,
        cast(order_item_row_discount_amount_gross_eur as numeric) as discount_eur,
        cast(is_promo_item as bool) as is_promo_item
    from {{ ref('fct_order_item') }}
),

cat_agg as (
    select
        category,
        sum(revenue_eur) as revenue_eur,
        sum(base_revenue_eur) as base_revenue_eur,
        sum(discount_eur) as discount_eur,
        sum(units) as units_sold,
        count(distinct order_sk) as orders,
        count(distinct customer_sk) as customers
    from base
    group by 1
),

totals as (
    select
        sum(revenue_eur) as total_revenue_eur,
        count(distinct order_sk) as total_orders,
        count(distinct customer_sk) as total_customers
    from base
),

promo_orders as (
    select
        category,
        countif(has_promo) as promo_orders
    from (
        select
            category,
            order_sk,
            max(case when is_promo_item then 1 else 0 end) = 1 as has_promo
        from base
        group by 1, 2
    )
    group by 1
),

repeat_stats as (
    select
        category,
        countif(order_cnt > 1) as repeat_customers
    from (
        select
            category,
            customer_sk,
            count(distinct order_sk) as order_cnt
        from base
        group by 1, 2
    )
    group by 1
),

sku_rev as (
    select
        category,
        product_name,
        sum(revenue_eur) as sku_revenue_eur
    from base
    group by 1, 2
),

sku_rank as (
    select
        *,
        row_number() over (
            partition by category
            order by sku_revenue_eur desc, product_name
        ) as rn
    from sku_rev
),

sku_shares as (
    select
        category,
        max(case when rn = 1 then sku_revenue_eur end) as top_sku_revenue_eur,
        sum(case when rn <= 3 then sku_revenue_eur else 0 end) as top_3_revenue_eur
    from sku_rank
    group by 1
)

select
    a.category,
    round(a.revenue_eur, 2) as revenue_eur,
    round(100 * safe_divide(a.revenue_eur, t.total_revenue_eur), 2) as share_pct,
    cast(round(a.units_sold, 0) as int64) as units,
    a.orders,
    a.customers,
    round(100 * safe_divide(a.orders, t.total_orders), 1) as order_pen_pct,
    round(100 * safe_divide(a.customers, t.total_customers), 1) as customer_pen_pct,
    round(safe_divide(a.revenue_eur, nullif(a.units_sold, 0)), 2) as asp_eur,
    round(100 * safe_divide(a.discount_eur, nullif(a.base_revenue_eur, 0)), 2) as disc_pct,
    round(100 * safe_divide(p.promo_orders, nullif(a.orders, 0)), 2) as promo_pct,
    round(safe_divide(a.revenue_eur, nullif(a.orders, 0)), 2) as rev_per_order_eur,
    round(safe_divide(a.orders, nullif(a.customers, 0)), 2) as orders_per_customer,
    round(100 * safe_divide(r.repeat_customers, nullif(a.customers, 0)), 1) as repeat_pct,
    round(100 * safe_divide(s.top_sku_revenue_eur, nullif(a.revenue_eur, 0)), 1) as top_sku_pct,
    round(100 * safe_divide(s.top_3_revenue_eur, nullif(a.revenue_eur, 0)), 1) as top_3_pct,
    t.total_revenue_eur,
    t.total_orders,
    t.total_customers,
    p.promo_orders,
    r.repeat_customers,
    s.top_sku_revenue_eur,
    s.top_3_revenue_eur
from cat_agg as a
cross join totals as t
left join promo_orders as p using (category)
left join repeat_stats as r using (category)
left join sku_shares as s using (category)
order by revenue_eur desc
