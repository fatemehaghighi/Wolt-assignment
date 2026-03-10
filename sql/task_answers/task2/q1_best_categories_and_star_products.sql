-- Task 2 - Q1
-- Which categories are performing better and what are the star products?

with latest_cat as (
    select *
    from `wolt-assignment-489610.analytics_dev_rpt.rpt_category_daily`
    where snapshot_date = (
        select max(snapshot_date)
        from `wolt-assignment-489610.analytics_dev_rpt.rpt_category_daily`
    )
),
category_rank as (
    select
        item_category,
        sum(order_item_rows_revenue_eur) as revenue_eur,
        sum(units_sold) as units_sold,
        avg(avg_selling_price_eur) as avg_selling_price_eur
    from latest_cat
    group by item_category
),
star_products as (
    select
        item_category,
        item_key,
        item_name_preferred,
        sum(order_item_row_final_amount_gross_eur) as revenue_eur,
        sum(units_in_order_item_row) as units_sold,
        row_number() over (
            partition by item_category
            order by sum(order_item_row_final_amount_gross_eur) desc
        ) as rn
    from `wolt-assignment-489610.analytics_dev_core.fct_order_item`
    group by item_category, item_key, item_name_preferred
)
select
    c.item_category,
    c.revenue_eur as category_revenue_eur,
    c.units_sold as category_units_sold,
    c.avg_selling_price_eur as category_avg_selling_price_eur,
    s.item_key as star_item_key,
    s.item_name_preferred as star_item_name,
    s.revenue_eur as star_item_revenue_eur,
    s.units_sold as star_item_units_sold
from category_rank as c
left join star_products as s
    on c.item_category = s.item_category
    and s.rn = 1
order by category_revenue_eur desc;
