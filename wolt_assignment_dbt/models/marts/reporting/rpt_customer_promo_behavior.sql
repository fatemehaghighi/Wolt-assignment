{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['customer_sk'],
        on_schema_change='sync_all_columns',
        cluster_by=['customer_sk'],
        pre_hook=ensure_run_metadata_table(),
        post_hook=upsert_run_metadata()
    )
}}

with order_level_item_mix as (
    select
        oi.customer_sk,
        oi.order_sk,
        any_value(o.customer_key) as customer_key,
        any_value(o.order_ts_utc) as order_ts_utc,
        any_value(o.order_ts_berlin) as order_ts_berlin,
        any_value(o.is_first_order_for_customer) as is_first_order_for_customer,
        -- Business need (Task 2 Q5): did customer consume promo units vs non-promo units?
        sum(case when oi.is_promo_item then oi.units_in_order_item_row else 0 end) as promo_units_purchased,
        sum(case when not oi.is_promo_item then oi.units_in_order_item_row else 0 end) as non_promo_units_purchased,
        sum(case when oi.is_promo_item then oi.order_item_row_final_amount_gross_eur else 0 end) as promo_value_eur,
        sum(case when not oi.is_promo_item then oi.order_item_row_final_amount_gross_eur else 0 end) as non_promo_value_eur
    from {{ ref('fct_order_item') }} as oi
    inner join {{ ref('fct_order') }} as o
        on oi.order_sk = o.order_sk
    group by oi.customer_sk, oi.order_sk
),
customer_rollup as (
    select
        customer_sk,
        any_value(customer_key) as customer_key,
        min(order_ts_utc) as first_order_ts_utc,
        min(order_ts_berlin) as first_order_ts_berlin,
        count(*) as lifetime_orders,
        -- Business need (Task 2 Q5): customer-level promo adoption pattern across orders.
        sum(cast(promo_units_purchased > 0 as int64)) as orders_with_any_promo_units,
        sum(cast(promo_units_purchased = 0 as int64)) as orders_with_no_promo_units,
        sum(promo_units_purchased) as promo_units_purchased,
        sum(non_promo_units_purchased) as non_promo_units_purchased,
        sum(promo_value_eur) as promo_value_eur,
        sum(non_promo_value_eur) as non_promo_value_eur,
        -- Business need (Task 2 Q5): first-order promo acquisition quality.
        max(cast(is_first_order_for_customer and promo_units_purchased > 0 as int64)) = 1 as first_order_had_any_promo_units,
        max(cast(is_first_order_for_customer and promo_units_purchased > 0 and non_promo_units_purchased = 0 as int64)) = 1 as first_order_had_only_promo_units
    from order_level_item_mix
    group by customer_sk
)
select
    customer_sk,
    customer_key,
    first_order_ts_utc,
    first_order_ts_berlin,
    first_order_had_any_promo_units,
    first_order_had_only_promo_units,
    orders_with_any_promo_units,
    orders_with_no_promo_units,
    promo_units_purchased,
    non_promo_units_purchased,
    promo_value_eur,
    non_promo_value_eur,
    promo_units_purchased > 0 and non_promo_units_purchased = 0 as promo_only_customer_flag,
    lifetime_orders
from customer_rollup
