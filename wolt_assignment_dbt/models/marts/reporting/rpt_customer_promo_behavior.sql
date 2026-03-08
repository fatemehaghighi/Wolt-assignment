select
    customer_sk,
    min(order_ts_utc) as first_order_ts_utc,
    max(cast(is_first_order_for_customer and contains_promo_flag as int64)) = 1 as first_order_had_promo,
    sum(cast(contains_promo_flag as int64)) as promo_orders,
    sum(cast(not contains_promo_flag as int64)) as non_promo_orders,
    sum(cast(contains_promo_flag as int64)) > 0 and sum(cast(not contains_promo_flag as int64)) = 0 as promo_only_customer_flag,
    count(*) as lifetime_orders
from {{ ref('fct_order') }}
group by customer_sk
