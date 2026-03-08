with bounds as (
    select
        min(order_date_berlin) as min_date,
        max(order_date_berlin) as max_date
    from {{ ref('stg_wolt_purchase_logs') }}
),
dates as (
    select d as date_day
    from bounds,
    unnest(generate_date_array(min_date, max_date)) as d
)
select
    date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(week from date_day) as week,
    format_date('%A', date_day) as day_of_week,
    extract(dayofweek from date_day) in (1, 7) as is_weekend
from dates
