{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['snapshot_date', 'date_day', 'item_category'],
        on_schema_change='sync_all_columns',
        partition_by={'field': 'snapshot_date', 'data_type': 'date'},
        cluster_by=['date_day', 'item_category'],
        pre_hook=ensure_run_metadata_table(),
        post_hook=upsert_run_metadata()
    )
}}

with item_daily as (
    select
        oi.order_date,
        coalesce(oi.item_category, 'Unknown') as item_category,
        -- Business need (Task 2 Q1/Q2): category performance trend by demand and value.
        count(distinct oi.order_sk) as orders,
        count(distinct oi.customer_sk) as customers,
        sum(oi.units_in_order_item_row) as units_sold,
        -- Business need (Task 2 Q5): track promo-driven category volume over time.
        sum(case when oi.is_promo_item then oi.units_in_order_item_row else 0 end) as promo_units_sold,
        sum(oi.order_item_row_final_amount_gross_eur) as order_item_rows_revenue_eur,
        sum(oi.order_item_row_discount_amount_gross_eur) as order_item_rows_discount_eur,
        -- Business need (Task 2 Q1): monitor value/mix shifts inside category.
        -- Use weighted ASP (revenue / units), not avg(unit price per row):
        -- this correctly weights high-quantity rows more than low-quantity rows.
        safe_divide(sum(oi.order_item_row_final_amount_gross_eur), nullif(sum(oi.units_in_order_item_row), 0)) as avg_selling_price_eur
    from {{ ref('fct_order_item') }} as oi
    group by 1, 2
),
order_daily as (
    select
        o.order_date,
        oi.item_category,
        -- Business need (Task 2 Q5): first-time vs repeat customer contribution by category/day.
        count(distinct case when o.is_first_order_for_customer then o.customer_sk end) as customers_whose_first_order_included_category,
        count(distinct case when not o.is_first_order_for_customer then o.customer_sk end) as customers_with_repeat_orders_including_category,
        -- Business need (Task 1 Q1): service area proxy by delivery distance over time.
        avg(o.delivery_distance_line_meters) as avg_delivery_distance_meters,
        -- Basket context for category demand quality.
        avg(o.total_units_in_order) as avg_order_units_for_orders_with_category
    from {{ ref('fct_order') }} as o
    inner join (
        select distinct order_sk, coalesce(item_category, 'Unknown') as item_category
        from {{ ref('fct_order_item') }}
    ) as oi
        on o.order_sk = oi.order_sk
    group by 1, 2
)
select
    {{ run_date_expr() }} as snapshot_date,
    i.order_date as date_day,
    i.item_category,
    i.orders,
    i.customers,
    coalesce(o.customers_whose_first_order_included_category, 0) as customers_whose_first_order_included_category,
    coalesce(o.customers_with_repeat_orders_including_category, 0) as customers_with_repeat_orders_including_category,
    i.units_sold,
    i.promo_units_sold,
    i.order_item_rows_revenue_eur,
    i.order_item_rows_discount_eur,
    i.avg_selling_price_eur,
    o.avg_order_units_for_orders_with_category,
    o.avg_delivery_distance_meters
from item_daily as i
left join order_daily as o
    on i.order_date = o.order_date
    and i.item_category = o.item_category
