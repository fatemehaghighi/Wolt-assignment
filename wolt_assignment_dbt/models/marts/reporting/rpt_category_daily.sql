{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['run_id', 'date_day', 'item_category'],
        on_schema_change='sync_all_columns',
        partition_by={'field': 'as_of_run_date', 'data_type': 'date'},
        cluster_by=['date_day', 'item_category'],
        pre_hook=ensure_run_metadata_table(),
        post_hook=upsert_run_metadata()
    )
}}

with item_daily as (
    select
        oi.order_date,
        coalesce(oi.item_category, 'Unknown') as item_category,
        count(distinct oi.order_sk) as orders,
        count(distinct oi.customer_sk) as customers,
        sum(oi.item_count) as items_sold,
        sum(case when oi.is_promo_item then oi.item_count else 0 end) as promo_items_sold,
        sum(oi.line_final_amount_gross_eur) as line_revenue_eur,
        sum(oi.line_discount_amount_gross_eur) as discount_eur,
        safe_divide(sum(oi.line_final_amount_gross_eur), nullif(sum(oi.item_count), 0)) as avg_selling_price_eur
    from {{ ref('fct_order_item') }} as oi
    group by 1, 2
),
order_daily as (
    select
        o.order_date,
        oi.item_category,
        count(distinct case when o.is_first_order_for_customer then o.customer_sk end) as first_time_customers,
        count(distinct case when not o.is_first_order_for_customer then o.customer_sk end) as repeat_customers,
        avg(o.delivery_distance_line_meters) as avg_delivery_distance_meters,
        avg(o.total_item_count) as avg_items_per_order
    from {{ ref('fct_order') }} as o
    inner join (
        select distinct order_sk, coalesce(item_category, 'Unknown') as item_category
        from {{ ref('fct_order_item') }}
    ) as oi
        on o.order_sk = oi.order_sk
    group by 1, 2
)
select
    {{ run_id_literal() }} as run_id,
    {{ run_ts_literal() }} as as_of_run_ts,
    {{ run_date_expr() }} as as_of_run_date,
    '{{ var('publish_tag', 'scheduled') }}' as publish_tag,
    i.order_date as date_day,
    i.item_category,
    i.orders,
    i.customers,
    coalesce(o.first_time_customers, 0) as first_time_customers,
    coalesce(o.repeat_customers, 0) as repeat_customers,
    i.items_sold,
    i.promo_items_sold,
    i.line_revenue_eur,
    i.discount_eur,
    i.avg_selling_price_eur,
    o.avg_items_per_order,
    o.avg_delivery_distance_meters
from item_daily as i
left join order_daily as o
    on i.order_date = o.order_date
    and i.item_category = o.item_category
