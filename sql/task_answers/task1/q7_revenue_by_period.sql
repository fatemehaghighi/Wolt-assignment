-- Task 1 - Q7
-- How much revenue has the company generated in any given period?
-- Revenue proxy in this assignment: total customer paid.

select
    order_date,
    count(*) as orders,
    sum(total_customer_paid_eur) as total_revenue_eur,
    sum(total_basket_value_eur) as basket_revenue_component_eur,
    sum(wolt_service_fee_eur) as wolt_fee_component_eur
from `wolt-assignment-489610.analytics_dev_core.fct_order`
group by order_date
order by order_date;
