{{
    config(
        materialized='table'
    )
}}

with base as (
    select
        coalesce(item_name_preferred, item_name_en, item_name_de) as product_name,
        nullif(trim(brand_name), '') as brand_name,
        order_sk,
        customer_sk,
        cast(units_in_order_item_row as numeric) as units,
        cast(order_item_row_final_amount_gross_eur as numeric) as revenue_eur,
        cast(order_item_row_base_amount_gross_eur as numeric) as base_revenue_eur,
        cast(order_item_row_discount_amount_gross_eur as numeric) as discount_eur
    from {{ ref('fct_order_item') }}
),

totals as (
    select sum(revenue_eur) as total_revenue_eur
    from base
),

product_agg as (
    select
        product_name,
        coalesce(any_value(brand_name), 'Unknown') as brand_name,
        sum(revenue_eur) as revenue_eur,
        sum(units) as units_sold,
        count(distinct order_sk) as orders,
        count(distinct customer_sk) as customers,
        sum(base_revenue_eur) as base_revenue_eur,
        sum(discount_eur) as discount_eur
    from base
    group by 1
),

ranked as (
    select
        row_number() over (order by p.revenue_eur desc, p.product_name) as product_rank,
        p.product_name,
        p.brand_name,
        p.revenue_eur,
        100 * safe_divide(p.revenue_eur, t.total_revenue_eur) as revenue_share_pct,
        p.units_sold,
        p.orders,
        p.customers,
        safe_divide(p.revenue_eur, nullif(p.units_sold, 0)) as asp_eur,
        100 * safe_divide(p.discount_eur, nullif(p.base_revenue_eur, 0)) as disc_pct
    from product_agg as p
    cross join totals as t
)

select
    product_rank as rank_no,
    product_name,
    brand_name,
    round(revenue_eur, 2) as revenue_eur,
    round(revenue_share_pct, 2) as share_pct,
    cast(round(units_sold, 0) as int64) as units,
    orders,
    customers,
    round(asp_eur, 2) as asp_eur,
    round(disc_pct, 2) as disc_pct,
    case
        when product_rank <= 2 then 'HERO'
        else null
    end as hero_flag
from ranked
where product_rank <= 10
order by product_rank
