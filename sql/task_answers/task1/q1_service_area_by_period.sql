-- Task 1 - Q1
-- What area is the store serving in any given period?
-- Service area proxy: delivery distance distribution by day.

select
    order_date,
    count(*) as orders,
    avg(delivery_distance_line_meters) as avg_delivery_distance_m,
    min(delivery_distance_line_meters) as min_delivery_distance_m,
    max(delivery_distance_line_meters) as max_delivery_distance_m,
    approx_quantiles(delivery_distance_line_meters, 100)[offset(50)] as p50_delivery_distance_m,
    approx_quantiles(delivery_distance_line_meters, 100)[offset(90)] as p90_delivery_distance_m
from `wolt-assignment-489610.analytics_dev_core.fct_order`
group by order_date
order by order_date;
