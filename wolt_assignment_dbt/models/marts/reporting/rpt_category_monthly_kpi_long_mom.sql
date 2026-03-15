{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['order_month', 'item_category', 'metric_name'],
        on_schema_change='sync_all_columns',
        partition_by={'field': 'order_month', 'data_type': 'date'},
        cluster_by=['metric_name', 'item_category']
    )
}}

with base as (
    -- Base long-format KPI table (one row per category-month-metric).
    select
        order_month,
        item_category,
        metric_name,
        cast(metric_value as float64) as metric_value
    from {{ ref('rpt_category_monthly_kpi_long') }}
),

with_mom as (
    select
        order_month,
        item_category,
        metric_name,
        metric_value,
        lag(metric_value) over (
            partition by item_category, metric_name
            order by order_month
        ) as previous_month_metric_value
    from base
)

select
    order_month,
    item_category,
    metric_name,
    metric_value,
    previous_month_metric_value,
    (metric_value - previous_month_metric_value) as mom_absolute_change,
    safe_divide(
        metric_value - previous_month_metric_value,
        nullif(previous_month_metric_value, 0)
    ) as mom_percentage_change
from with_mom
