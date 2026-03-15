{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_sk',
        on_schema_change='sync_all_columns',
        pre_hook=[ensure_watermark_table()] if var('enable_watermark_checks', true) else [],
        post_hook=[
            upsert_model_watermark(
                'fct_order',
                'order_ts_utc',
                '`' ~ target.database ~ '`.`' ~ target.schema ~ '_core`.`fct_order`'
            )
        ] if var('enable_watermark_checks', true) else [],
        partition_by={'field': 'order_date', 'data_type': 'date'},
        cluster_by=['order_sk', 'customer_sk']
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
    -- Assignment is Berlin-specific and business analysis is local-time based.
    -- Expose Berlin local order date as canonical `order_date` for marts/reporting.
    order_date_berlin as order_date,
    extract(hour from time_order_received_utc) as order_hour_utc,
    extract(hour from datetime(time_order_received_utc, 'Europe/Berlin')) as order_hour_berlin,
    format_date('%A', order_date_berlin) as order_day_name,
    delivery_distance_line_meters,
    -- Task 1: order-level fee/cost/revenue viability analysis inputs.
    total_basket_value_eur,
    wolt_service_fee_eur,
    courier_base_fee_eur,
    -- = total_basket_value_eur + wolt_service_fee_eur
    basket_plus_service_fee_eur,
    -- = total_basket_value_eur + wolt_service_fee_eur + courier_base_fee_eur
    total_customer_paid_eur,
    total_units_in_order,
    distinct_order_item_rows_in_order,
    promo_order_item_rows_in_order,
    promo_units_in_order,
    has_any_promo_units_in_order,
    derived_order_items_base_amount_gross_eur,
    derived_order_items_discount_amount_gross_eur,
    derived_order_items_final_amount_gross_eur,
    -- Per-customer order sequence by order_ts_utc (ties by purchase_key).
    customer_order_number,
    -- Task 1/2 customer lifecycle signal (repeat vs first-time behavior).
    is_first_order_for_customer
from {{ ref('int_wolt_orders_with_item_rollups') }}
{% if is_incremental() %}
where time_order_received_utc >= (
    {{ incremental_cutoff_expr('fct_order', 'order_ts_utc') }}
)
{% endif %}
