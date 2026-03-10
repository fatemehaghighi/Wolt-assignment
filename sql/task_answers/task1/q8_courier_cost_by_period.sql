-- Task 1 - Q8
-- How much are courier costs in any given period?

select
    order_date,
    count(*) as orders,
    sum(courier_base_fee_eur) as courier_cost_eur,
    avg(courier_base_fee_eur) as avg_courier_cost_per_order_eur
from `wolt-assignment-489610.analytics_dev_core.fct_order`
group by order_date
order by order_date;
