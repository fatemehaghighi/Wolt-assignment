{{ config(materialized='table') }}

with base as (
    select
        oi.item_category,
        date_trunc(oi.order_date, month) as order_month,
        extract(dayofweek from date(o.order_ts_berlin)) as day_of_week_berlin_num,
        case extract(dayofweek from date(o.order_ts_berlin))
            when 1 then 'Sunday'
            when 2 then 'Monday'
            when 3 then 'Tuesday'
            when 4 then 'Wednesday'
            when 5 then 'Thursday'
            when 6 then 'Friday'
            when 7 then 'Saturday'
        end as day_of_week_berlin,
        extract(hour from o.order_ts_berlin) as order_hour_berlin,
        oi.units_in_order_item_row as units_sold,
        oi.order_item_row_final_amount_gross_eur as revenue_eur
    from {{ ref('fct_order_item') }} as oi
    inner join {{ ref('fct_order') }} as o
        on oi.order_sk = o.order_sk
    where oi.item_category is not null
),

bucketed as (
    select
        order_month,
        item_category,
        day_of_week_berlin_num,
        day_of_week_berlin,
        order_hour_berlin,
        case
            when order_hour_berlin between 6 and 10 then 'Morning (06-10)'
            when order_hour_berlin between 11 and 16 then 'Afternoon (11-16)'
            when order_hour_berlin between 17 and 22 then 'Evening (17-22)'
            else 'Night (23-05)'
        end as daypart_berlin,
        units_sold,
        revenue_eur
    from base
),

agg as (
    select
        order_month,
        item_category,
        day_of_week_berlin_num,
        day_of_week_berlin,
        order_hour_berlin,
        daypart_berlin,
        sum(revenue_eur) as revenue_eur,
        sum(units_sold) as units_sold
    from bucketed
    group by 1, 2, 3, 4, 5, 6
)

select
    order_month,
    item_category,
    day_of_week_berlin_num,
    day_of_week_berlin,
    order_hour_berlin,
    daypart_berlin,
    revenue_eur,
    units_sold
from agg
order by
    order_month,
    item_category,
    day_of_week_berlin_num,
    order_hour_berlin
