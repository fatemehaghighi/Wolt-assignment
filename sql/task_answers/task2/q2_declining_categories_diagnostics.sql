-- Task 2 - Q2
-- Among categories not improving, what is going on?
-- Month-over-month diagnostics for categories with revenue decline.

with latest_cat as (
    select *
    from `wolt-assignment-489610.analytics_dev_rpt.rpt_category_monthly`
),
monthly as (
    select
        order_month,
        item_category,
        order_item_rows_revenue_eur as revenue_eur,
        units_sold,
        promo_units_sold,
        weighted_avg_selling_price_eur,
        avg_order_units_for_orders_with_category as avg_order_units
    from latest_cat
),
with_lag as (
    select
        *,
        lag(revenue_eur) over (partition by item_category order by order_month) as prev_revenue_eur,
        lag(units_sold) over (partition by item_category order by order_month) as prev_units_sold,
        lag(weighted_avg_selling_price_eur) over (partition by item_category order by order_month) as prev_weighted_avg_selling_price_eur
    from monthly
)
select
    order_month,
    item_category,
    revenue_eur,
    prev_revenue_eur,
    revenue_eur - prev_revenue_eur as revenue_mom_change_eur,
    units_sold,
    prev_units_sold,
    units_sold - prev_units_sold as units_mom_change,
    weighted_avg_selling_price_eur,
    prev_weighted_avg_selling_price_eur,
    weighted_avg_selling_price_eur - prev_weighted_avg_selling_price_eur as weighted_asp_mom_change_eur,
    promo_units_sold,
    safe_divide(promo_units_sold, nullif(units_sold, 0)) as promo_unit_share,
    avg_order_units
from with_lag
where prev_revenue_eur is not null
  and revenue_eur < prev_revenue_eur
order by order_month desc, revenue_mom_change_eur asc;
