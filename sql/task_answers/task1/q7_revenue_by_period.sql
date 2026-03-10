-- Task 1 - Q7
-- How much revenue has the company generated in any given period?
-- Revenue metric definitions:
--   basket_value_eur: item sales paid by customer (after promo discounts)
--   wolt_service_fee_eur: platform fee paid by customer
--   courier_base_fee_eur: delivery fee component paid by customer
--   total_customer_paid_eur: basket + service + courier (used as revenue proxy)

select
    order_date,
    count(*) as orders,
    sum(total_customer_paid_eur) as total_customer_paid_revenue_proxy_eur,
    sum(total_basket_value_eur) as basket_revenue_component_eur,
    sum(wolt_service_fee_eur) as wolt_fee_component_eur,
    sum(courier_base_fee_eur) as courier_fee_component_eur
from `wolt-assignment-489610.analytics_dev_core.fct_order`
group by order_date
order by order_date;
