-- Task 2 - Q2
-- Among categories not improving, what is going on?
-- Month-over-month diagnostics for categories with revenue decline.

with latest_cat as (
    select *
    from `wolt-assignment-489610.analytics_dev_rpt.rpt_category_daily`
    where snapshot_date = (
        select max(snapshot_date)
        from `wolt-assignment-489610.analytics_dev_rpt.rpt_category_daily`
    )
),
monthly as (
    select
        date_trunc(date_day, month) as month_start,
        item_category,
        sum(order_item_rows_revenue_eur) as revenue_eur,
        sum(units_sold) as units_sold,
        sum(promo_units_sold) as promo_units_sold,
        -- Weighted ASP across days in month: total revenue / total units.
        safe_divide(sum(order_item_rows_revenue_eur), nullif(sum(units_sold), 0)) as avg_selling_price_eur,
        avg(avg_order_units_for_orders_with_category) as avg_order_units
    from latest_cat
    group by month_start, item_category
),
with_lag as (
    select
        *,
        lag(revenue_eur) over (partition by item_category order by month_start) as prev_revenue_eur,
        lag(units_sold) over (partition by item_category order by month_start) as prev_units_sold,
        lag(avg_selling_price_eur) over (partition by item_category order by month_start) as prev_avg_selling_price_eur
    from monthly
)
select
    month_start,
    item_category,
    revenue_eur,
    prev_revenue_eur,
    revenue_eur - prev_revenue_eur as revenue_mom_change_eur,
    units_sold,
    prev_units_sold,
    units_sold - prev_units_sold as units_mom_change,
    avg_selling_price_eur,
    prev_avg_selling_price_eur,
    avg_selling_price_eur - prev_avg_selling_price_eur as asp_mom_change_eur,
    promo_units_sold,
    safe_divide(promo_units_sold, nullif(units_sold, 0)) as promo_unit_share,
    avg_order_units
from with_lag
where prev_revenue_eur is not null
  and revenue_eur < prev_revenue_eur
order by month_start desc, revenue_mom_change_eur asc;
