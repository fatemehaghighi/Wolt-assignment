with customer_orders as (
    select
        customer_sk,
        order_ts_utc,
        order_date,
        total_basket_value_eur,
        contains_promo_flag,
        is_first_order_for_customer
    from {{ ref('fct_order') }}
)
select
    customer_sk,
    min(order_ts_utc) as first_order_ts_utc,
    max(order_ts_utc) as last_order_ts_utc,
    count(*) as lifetime_orders,
    sum(total_basket_value_eur) as lifetime_basket_value_eur,
    sum(cast(contains_promo_flag as int64)) as orders_with_promo,
    max(cast(is_first_order_for_customer and contains_promo_flag as int64)) = 1 as first_order_contains_promo_flag
from customer_orders
group by customer_sk
