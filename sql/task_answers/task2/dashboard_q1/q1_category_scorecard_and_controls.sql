-- Task 2 Q1 dashboard dataset (scorecard): category ranking + risk/control flags.
-- Purpose: classify category performance using growth + quality + concentration + volatility.

with month_category as (
    select
        order_month as period_month,
        item_category,
        order_item_rows_revenue_eur as revenue_eur,
        units_sold,
        orders as orders_with_category,
        promo_units_sold,
        distinct_customers_whose_first_order_included_category_in_month as first_order_customers_attr,
        distinct_customers_with_repeat_orders_including_category_in_month as repeat_order_customers_attr
    from `wolt-assignment-489610.analytics_dev_rpt.rpt_category_monthly`
    where order_month between date '2023-01-01' and date '2023-12-01'
),
by_cat as (
    select
        item_category,
        min(period_month) as first_month,
        max(period_month) as last_month,
        sum(revenue_eur) as total_revenue_eur,
        sum(units_sold) as total_units_sold,
        sum(orders_with_category) as total_orders_with_category,
        safe_divide(sum(promo_units_sold), nullif(sum(units_sold), 0)) as promo_unit_share,
        safe_divide(sum(repeat_order_customers_attr), nullif(sum(first_order_customers_attr + repeat_order_customers_attr), 0)) as repeat_customer_mix_ratio,
        safe_divide(sum(revenue_eur), nullif(sum(units_sold), 0)) as weighted_asp_eur,
        avg(revenue_eur) as avg_monthly_revenue_eur,
        stddev_pop(revenue_eur) as std_monthly_revenue_eur
    from month_category
    group by 1
),
first_last as (
    select
        m.item_category,
        max(case when m.period_month = b.first_month then m.revenue_eur end) as first_month_revenue_eur,
        max(case when m.period_month = b.last_month then m.revenue_eur end) as last_month_revenue_eur,
        max(case when m.period_month = b.first_month then m.units_sold end) as first_month_units,
        max(case when m.period_month = b.last_month then m.units_sold end) as last_month_units,
        max(case when m.period_month = b.first_month then m.orders_with_category end) as first_month_orders,
        max(case when m.period_month = b.last_month then m.orders_with_category end) as last_month_orders
    from month_category m
    join by_cat b using(item_category)
    group by 1
),
star as (
    select
        item_category,
        item_key,
        any_value(item_name_preferred) as item_name_preferred,
        sum(order_item_row_final_amount_gross_eur) as item_revenue_eur,
        row_number() over (partition by item_category order by sum(order_item_row_final_amount_gross_eur) desc) as rn
    from `wolt-assignment-489610.analytics_dev_core.fct_order_item`
    where order_date between date '2023-01-01' and date '2023-12-31'
    group by 1, 2
)
select
    b.item_category,
    b.total_revenue_eur,
    b.total_units_sold,
    b.total_orders_with_category,
    b.weighted_asp_eur,
    b.promo_unit_share,
    b.repeat_customer_mix_ratio,
    safe_divide(fl.last_month_revenue_eur - fl.first_month_revenue_eur, nullif(fl.first_month_revenue_eur, 0)) as revenue_growth_ratio_first_to_last,
    safe_divide(fl.last_month_units - fl.first_month_units, nullif(fl.first_month_units, 0)) as unit_growth_ratio_first_to_last,
    safe_divide(fl.last_month_orders - fl.first_month_orders, nullif(fl.first_month_orders, 0)) as order_growth_ratio_first_to_last,
    safe_divide(b.std_monthly_revenue_eur, nullif(b.avg_monthly_revenue_eur, 0)) as revenue_cv,
    s.item_key as star_item_key,
    s.item_name_preferred as star_item_name,
    s.item_revenue_eur as star_item_revenue_eur,
    safe_divide(s.item_revenue_eur, nullif(b.total_revenue_eur, 0)) as star_item_revenue_share,
    case when safe_divide(s.item_revenue_eur, nullif(b.total_revenue_eur, 0)) >= 0.65 then 1 else 0 end as control_high_star_concentration_flag,
    case when safe_divide(b.std_monthly_revenue_eur, nullif(b.avg_monthly_revenue_eur, 0)) >= 0.60 then 1 else 0 end as control_high_revenue_volatility_flag,
    case when b.promo_unit_share >= 0.12 then 1 else 0 end as control_high_promo_dependency_flag,
    case when b.repeat_customer_mix_ratio <= 0.95 then 1 else 0 end as control_low_repeat_mix_flag
from by_cat b
join first_last fl using(item_category)
left join star s on b.item_category = s.item_category and s.rn = 1
order by b.total_revenue_eur desc;
