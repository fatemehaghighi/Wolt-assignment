{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_sk',
        on_schema_change='sync_all_columns',
        pre_hook=[ensure_watermark_table()] if var('enable_watermark_checks', true) else [],
        post_hook=[upsert_model_watermark('fct_order', 'order_ts_utc')] if var('enable_watermark_checks', true) else [],
        partition_by={'field': 'order_date', 'data_type': 'date'},
        cluster_by=['customer_sk', 'order_sk', 'contains_promo_flag']
    )
}}

select
    order_sk,
    customer_sk,
    purchase_key,
    customer_key,
    time_order_received_utc as order_ts_utc,
    datetime(time_order_received_utc, 'Europe/Berlin') as order_ts_berlin,
    order_date_utc,
    order_date_berlin as order_date,
    extract(hour from time_order_received_utc) as order_hour_utc,
    extract(hour from datetime(time_order_received_utc, 'Europe/Berlin')) as order_hour_berlin,
    format_date('%A', order_date_berlin) as order_day_name,
    delivery_distance_line_meters,
    total_basket_value_eur,
    wolt_service_fee_eur,
    courier_base_fee_eur,
    basket_plus_service_fee_eur,
    total_customer_paid_eur,
    total_item_count,
    distinct_item_count,
    promo_line_count,
    promo_item_count,
    contains_promo_flag,
    model_line_base_amount_gross_eur,
    model_line_discount_amount_gross_eur,
    model_line_final_amount_gross_eur,
    customer_order_number,
    is_first_order_for_customer
from {{ ref('int_wolt_orders_enriched') }}
{% if is_incremental() %}
where time_order_received_utc >= (
    timestamp_sub(
        {% if var('enable_watermark_checks', true) %}
            {{ watermark_lookup_expr('fct_order') }}
        {% else %}
            (
                select coalesce(
                    max(order_ts_utc),
                    timestamp('1900-01-01 00:00:00+00')
                )
                from {{ this }}
            )
        {% endif %},
        interval {{ var('incremental_lookback_days', 7) }} day
    )
)
{% endif %}
