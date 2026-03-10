-- Task 1 - Q2
-- What items are being bought and what price are they going for in any given period?

select
    order_date,
    item_key,
    item_name_preferred,
    item_category,
    sum(units_in_order_item_row) as units_sold,
    safe_divide(
        sum(order_item_row_final_amount_gross_eur),
        nullif(sum(units_in_order_item_row), 0)
    ) as weighted_avg_selling_price_eur,
    safe_divide(
        sum(order_item_row_base_amount_gross_eur),
        nullif(sum(units_in_order_item_row), 0)
    ) as weighted_avg_base_price_eur
from `wolt-assignment-489610.analytics_dev_core.fct_order_item`
group by order_date, item_key, item_name_preferred, item_category
order by order_date, units_sold desc;
