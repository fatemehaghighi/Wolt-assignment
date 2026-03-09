{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_sk',
        on_schema_change='sync_all_columns',
        pre_hook=[ensure_watermark_table()] if var('enable_watermark_checks', true) else [],
        post_hook=[upsert_model_watermark('dim_customer', 'last_order_ts_utc')] if var('enable_watermark_checks', true) else [],
        cluster_by=['customer_sk', 'customer_key']
    )
}}

with source_orders as (
    select
        customer_sk,
        customer_key,
        order_ts_utc,
        order_date,
        total_basket_value_eur,
        contains_promo_flag,
        is_first_order_for_customer
    from {{ ref('fct_order') }}
),
affected_customers as (
    {% if is_incremental() %}
        select distinct customer_sk
        from source_orders
        where order_ts_utc >= (
            timestamp_sub(
                {% if var('enable_watermark_checks', true) %}
                    {{ watermark_lookup_expr('dim_customer') }}
                {% else %}
                    (
                        select coalesce(
                            max(last_order_ts_utc),
                            timestamp('1900-01-01 00:00:00+00')
                        )
                        from {{ this }}
                    )
                {% endif %},
                interval {{ var('incremental_lookback_days', 7) }} day
            )
        )
    {% else %}
        select distinct customer_sk
        from source_orders
    {% endif %}
),
customer_orders as (
    select s.*
    from source_orders as s
    inner join affected_customers as a
        on s.customer_sk = a.customer_sk
)
select
    customer_sk,
    any_value(customer_key) as customer_key,
    min(order_ts_utc) as first_order_ts_utc,
    max(order_ts_utc) as last_order_ts_utc,
    count(*) as lifetime_orders,
    sum(total_basket_value_eur) as lifetime_basket_value_eur,
    sum(cast(contains_promo_flag as int64)) as orders_with_promo,
    max(cast(is_first_order_for_customer and contains_promo_flag as int64)) = 1 as first_order_contains_promo_flag
from customer_orders
group by customer_sk
