-- Task 1 - Q5
-- Are customers coming back to the store?

with customer_summary as (
    select
        customer_sk,
        count(*) as orders_count,
        min(order_date) as first_order_date,
        max(order_date) as latest_order_date
    from `wolt-assignment-489610.analytics_dev_core.fct_order`
    group by customer_sk
)
select
    count(*) as total_customers,
    countif(orders_count = 1) as one_time_customers,
    countif(orders_count > 1) as repeat_customers,
    safe_divide(countif(orders_count > 1), count(*)) as repeat_customer_rate,
    avg(orders_count) as avg_orders_per_customer
from customer_summary;
