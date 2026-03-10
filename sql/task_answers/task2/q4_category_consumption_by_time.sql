-- Task 2 - Q4
-- How do users consume each category in different periods of time?
-- Hour-of-day and day-of-week consumption by category.

select
    o.order_day_name,
    o.order_hour_berlin,
    oi.item_category,
    count(distinct oi.order_sk) as orders,
    count(distinct oi.customer_sk) as customers,
    sum(oi.units_in_order_item_row) as units_sold,
    sum(oi.order_item_row_final_amount_gross_eur) as revenue_eur
from `wolt-assignment-489610.analytics_dev_core.fct_order_item` as oi
inner join `wolt-assignment-489610.analytics_dev_core.fct_order` as o
    on oi.order_sk = o.order_sk
group by o.order_day_name, o.order_hour_berlin, oi.item_category
order by oi.item_category, o.order_day_name, o.order_hour_berlin;
