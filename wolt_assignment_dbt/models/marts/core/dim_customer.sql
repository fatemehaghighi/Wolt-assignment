{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='customer_sk',
        on_schema_change='sync_all_columns',
        pre_hook=[ensure_watermark_table()] if var('enable_watermark_checks', true) else [],
        post_hook=[
            upsert_model_watermark(
                'dim_customer',
                'last_order_ts_utc',
                '`' ~ target.database ~ '`.`' ~ target.schema ~ '_core`.`dim_customer`'
            )
        ] if var('enable_watermark_checks', true) else [],
        cluster_by=['customer_sk']
    )
}}

with source_orders as (
    select
        customer_sk,
        customer_key,
        order_ts_utc,
        order_date,
        total_basket_value_eur,
        has_any_promo_units_in_order,
        is_first_order_for_customer
    from {{ ref('fct_order') }}
),
affected_customers as (
    {% if is_incremental() %}
        select distinct customer_sk
        from source_orders
        where order_ts_utc >= (
            {{ incremental_cutoff_expr('dim_customer', 'last_order_ts_utc') }}
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
    -- Earliest known order timestamp for the customer.
    min(order_ts_utc) as first_order_ts_utc,
    -- Latest known order timestamp for watermarking and recency analysis.
    max(order_ts_utc) as last_order_ts_utc,
    -- Total number of orders placed by the customer.
    count(*) as lifetime_orders,
    -- Lifetime basket value excluding service/courier fees.
    sum(total_basket_value_eur) as lifetime_basket_value_eur,
    -- Number of orders with at least one promo unit.
    sum(cast(has_any_promo_units_in_order as int64)) as orders_with_promo,
    -- True if the customer's first order had at least one promo unit.
    max(cast(is_first_order_for_customer and has_any_promo_units_in_order as int64)) = 1 as first_order_contains_promo_flag
from customer_orders
group by customer_sk
