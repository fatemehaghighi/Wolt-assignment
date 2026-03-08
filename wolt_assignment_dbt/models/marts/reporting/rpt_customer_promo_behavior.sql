{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['run_id', 'customer_sk'],
        on_schema_change='sync_all_columns',
        partition_by={'field': 'as_of_run_date', 'data_type': 'date'},
        cluster_by=['customer_sk']
    )
}}

with order_level_item_mix as (
    select
        oi.customer_sk,
        oi.order_sk,
        any_value(o.order_ts_utc) as order_ts_utc,
        any_value(o.is_first_order_for_customer) as is_first_order_for_customer,
        sum(case when oi.is_promo_item then oi.item_count else 0 end) as promo_item_count,
        sum(case when not oi.is_promo_item then oi.item_count else 0 end) as non_promo_item_count,
        sum(case when oi.is_promo_item then oi.line_final_amount_gross_eur else 0 end) as promo_item_value_eur,
        sum(case when not oi.is_promo_item then oi.line_final_amount_gross_eur else 0 end) as non_promo_item_value_eur
    from {{ ref('fct_order_item') }} as oi
    inner join {{ ref('fct_order') }} as o
        on oi.order_sk = o.order_sk
    group by oi.customer_sk, oi.order_sk
),
customer_rollup as (
    select
        customer_sk,
        min(order_ts_utc) as first_order_ts_utc,
        count(*) as lifetime_orders,
        sum(cast(promo_item_count > 0 as int64)) as promo_orders,
        sum(cast(promo_item_count = 0 as int64)) as non_promo_orders,
        sum(promo_item_count) as promo_item_count,
        sum(non_promo_item_count) as non_promo_item_count,
        sum(promo_item_value_eur) as promo_item_value_eur,
        sum(non_promo_item_value_eur) as non_promo_item_value_eur,
        max(cast(is_first_order_for_customer and promo_item_count > 0 as int64)) = 1 as first_order_had_any_promo_item,
        max(cast(is_first_order_for_customer and promo_item_count > 0 and non_promo_item_count = 0 as int64)) = 1 as first_order_had_only_promo_items
    from order_level_item_mix
    group by customer_sk
)
select
    {{ run_id_literal() }} as run_id,
    {{ run_ts_literal() }} as as_of_run_ts,
    {{ run_date_expr() }} as as_of_run_date,
    '{{ var('publish_tag', 'scheduled') }}' as publish_tag,
    customer_sk,
    first_order_ts_utc,
    first_order_had_any_promo_item,
    first_order_had_only_promo_items,
    promo_orders,
    non_promo_orders,
    promo_item_count,
    non_promo_item_count,
    promo_item_value_eur,
    non_promo_item_value_eur,
    promo_item_count > 0 and non_promo_item_count = 0 as promo_only_customer_flag,
    lifetime_orders
from customer_rollup
