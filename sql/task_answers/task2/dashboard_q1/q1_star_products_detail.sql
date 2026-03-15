-- Task 2 Q1 dashboard dataset: top products within category (star products + concentration).

with base as (
    select
        item_category,
        item_key,
        any_value(item_name_preferred) as item_name_preferred,
        sum(order_item_row_final_amount_gross_eur) as revenue_eur,
        sum(units_in_order_item_row) as units_sold,
        count(distinct order_sk) as orders_count
    from `wolt-assignment-489610.analytics_dev_core.fct_order_item`
    where order_date between date '2023-01-01' and date '2023-12-31'
    group by 1, 2
),
cat_totals as (
    select
        item_category,
        sum(revenue_eur) as category_revenue_eur,
        sum(units_sold) as category_units_sold
    from base
    group by 1
)
select
    b.item_category,
    b.item_key,
    b.item_name_preferred,
    b.revenue_eur,
    b.units_sold,
    b.orders_count,
    safe_divide(b.revenue_eur, nullif(c.category_revenue_eur, 0)) as revenue_share_in_category,
    safe_divide(b.units_sold, nullif(c.category_units_sold, 0)) as unit_share_in_category,
    row_number() over (partition by b.item_category order by b.revenue_eur desc) as revenue_rank_in_category
from base b
join cat_totals c using(item_category)
qualify revenue_rank_in_category <= 10
order by item_category, revenue_rank_in_category;
