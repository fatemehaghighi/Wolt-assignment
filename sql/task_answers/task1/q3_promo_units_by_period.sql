-- Task 1 - Q3
-- How many items are being bought on promotion in any given period?

select
    order_date,
    sum(case when is_promo_item then units_in_order_item_row else 0 end) as promo_units_sold,
    sum(units_in_order_item_row) as total_units_sold,
    safe_divide(
        sum(case when is_promo_item then units_in_order_item_row else 0 end),
        nullif(sum(units_in_order_item_row), 0)
    ) as promo_unit_share
from `wolt-assignment-489610.analytics_dev_core.fct_order_item`
group by order_date
order by order_date;
