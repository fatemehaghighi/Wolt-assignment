-- Task 1 - Q4
-- Are customers taking advantage of promotions?
-- Customer-level view.

select
    customer_sk,
    customer_key,
    lifetime_orders,
    orders_with_any_promo_units,
    orders_with_no_promo_units,
    promo_units_purchased,
    non_promo_units_purchased,
    promo_value_eur,
    non_promo_value_eur,
    safe_divide(orders_with_any_promo_units, nullif(lifetime_orders, 0)) as promo_order_rate,
    safe_divide(promo_units_purchased, nullif(promo_units_purchased + non_promo_units_purchased, 0)) as promo_unit_share,
    promo_only_customer_flag
from `wolt-assignment-489610.analytics_dev_rpt.rpt_customer_promo_behavior`
where snapshot_date = (
    select max(snapshot_date)
    from `wolt-assignment-489610.analytics_dev_rpt.rpt_customer_promo_behavior`
)
order by promo_order_rate desc, promo_units_purchased desc;
