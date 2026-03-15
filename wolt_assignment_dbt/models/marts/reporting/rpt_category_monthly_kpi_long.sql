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

with src as (
    select *
    from {{ ref('rpt_category_monthly') }}
)
select order_month, item_category, 'Revenue EUR' as metric_name, cast(revenue_eur as float64) as metric_value from src
union all
select order_month, item_category, 'Base Revenue EUR' as metric_name, cast(base_revenue_eur as float64) as metric_value from src
union all
select order_month, item_category, 'Discount EUR' as metric_name, cast(discount_eur as float64) as metric_value from src
union all
select order_month, item_category, 'Discount Rate' as metric_name, cast(discount_rate as float64) as metric_value from src
union all
select order_month, item_category, 'Units' as metric_name, cast(units_sold as float64) as metric_value from src
union all
select order_month, item_category, 'Orders' as metric_name, cast(orders as float64) as metric_value from src
union all
select order_month, item_category, 'Customers' as metric_name, cast(customers as float64) as metric_value from src
union all
select order_month, item_category, 'Revenue Share' as metric_name, cast(revenue_share as float64) as metric_value from src
union all
select order_month, item_category, 'Order Penetration' as metric_name, cast(order_penetration as float64) as metric_value from src
union all
select order_month, item_category, 'Customer Penetration' as metric_name, cast(customer_penetration as float64) as metric_value from src
union all
select order_month, item_category, 'ASP EUR' as metric_name, cast(asp_eur as float64) as metric_value from src
union all
select order_month, item_category, 'Revenue Per Order EUR' as metric_name, cast(revenue_per_order_eur as float64) as metric_value from src
union all
select order_month, item_category, 'Units Per Order' as metric_name, cast(units_per_order as float64) as metric_value from src
union all
select order_month, item_category, 'Orders Per Customer' as metric_name, cast(orders_per_customer as float64) as metric_value from src
union all
select order_month, item_category, 'Repeat Customer Rate' as metric_name, cast(repeat_customer_rate as float64) as metric_value from src
union all
select order_month, item_category, 'SKU Count' as metric_name, cast(sku_count as float64) as metric_value from src
union all
select order_month, item_category, 'Top SKU Share' as metric_name, cast(top_sku_share as float64) as metric_value from src
union all
select order_month, item_category, 'Top 3 Share' as metric_name, cast(top_3_share as float64) as metric_value from src
union all
select order_month, item_category, 'Cross-sell Attach Rate' as metric_name, cast(cross_sell_attach_rate as float64) as metric_value from src
