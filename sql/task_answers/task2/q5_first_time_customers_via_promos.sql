-- Task 2 - Q5
-- Do we get many first-time customers through promotions?
-- Do they only make purchases of promo units?

with latest as (
    select *
    from `wolt-assignment-489610.analytics_dev_rpt.rpt_customer_promo_behavior`
    where snapshot_date = (
        select max(snapshot_date)
        from `wolt-assignment-489610.analytics_dev_rpt.rpt_customer_promo_behavior`
    )
)
select
    count(*) as customers_total,
    countif(first_order_had_any_promo_units) as customers_first_order_with_promo_units,
    safe_divide(countif(first_order_had_any_promo_units), count(*)) as first_order_promo_acquisition_rate,
    countif(first_order_had_only_promo_units) as customers_first_order_only_promo_units,
    safe_divide(countif(first_order_had_only_promo_units), count(*)) as first_order_only_promo_rate,
    countif(promo_only_customer_flag) as customers_always_promo_only,
    safe_divide(countif(promo_only_customer_flag), count(*)) as lifetime_promo_only_customer_rate
from latest;
