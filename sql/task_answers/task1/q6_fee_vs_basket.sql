-- Task 1 - Q6
-- How do Wolt and courier fees compare to basket value?

select
    order_date,
    sum(total_basket_value_eur) as basket_value_eur,
    sum(wolt_service_fee_eur) as wolt_service_fee_eur,
    sum(courier_base_fee_eur) as courier_base_fee_eur,
    safe_divide(sum(wolt_service_fee_eur), nullif(sum(total_basket_value_eur), 0)) as wolt_fee_to_basket_ratio,
    safe_divide(sum(courier_base_fee_eur), nullif(sum(total_basket_value_eur), 0)) as courier_fee_to_basket_ratio,
    safe_divide(sum(wolt_service_fee_eur + courier_base_fee_eur), nullif(sum(total_basket_value_eur), 0)) as total_fee_to_basket_ratio
from `wolt-assignment-489610.analytics_dev_core.fct_order`
group by order_date
order by order_date;
