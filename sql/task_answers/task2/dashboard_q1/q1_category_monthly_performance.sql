-- Task 2 Q1 dashboard dataset (chart-level): monthly category performance trends.
-- Purpose: show size + growth + quality mix by category over time.

with base as (
    select
        order_month as period_month,
        item_category,
        order_item_rows_revenue_eur as revenue_eur,
        units_sold,
        orders as orders_with_category,
        promo_units_sold,
        distinct_customers_whose_first_order_included_category_in_month as first_order_customers_attr,
        distinct_customers_with_repeat_orders_including_category_in_month as repeat_order_customers_attr,
        weighted_avg_selling_price_eur as weighted_asp_eur
    from `wolt-assignment-489610.analytics_dev_rpt.rpt_category_monthly`
    where order_month between date '2023-01-01' and date '2023-12-01'
),
with_lag as (
    select
        *,
        lag(revenue_eur) over (partition by item_category order by period_month) as prev_revenue_eur,
        lag(units_sold) over (partition by item_category order by period_month) as prev_units_sold,
        lag(orders_with_category) over (partition by item_category order by period_month) as prev_orders_with_category
    from base
)
select
    period_month,
    item_category,
    revenue_eur,
    units_sold,
    orders_with_category,
    weighted_asp_eur,
    safe_divide(promo_units_sold, nullif(units_sold, 0)) as promo_unit_share,
    safe_divide(repeat_order_customers_attr, nullif(first_order_customers_attr + repeat_order_customers_attr, 0)) as repeat_customer_mix_ratio,
    safe_divide(revenue_eur - prev_revenue_eur, nullif(prev_revenue_eur, 0)) as revenue_mom_growth_ratio,
    safe_divide(units_sold - prev_units_sold, nullif(prev_units_sold, 0)) as units_mom_growth_ratio,
    safe_divide(orders_with_category - prev_orders_with_category, nullif(prev_orders_with_category, 0)) as orders_mom_growth_ratio
from with_lag
order by item_category, period_month;
