{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_item_sk',
        on_schema_change='sync_all_columns',
        pre_hook=[ensure_watermark_table()] if var('enable_watermark_checks', true) else [],
        post_hook=[upsert_model_watermark('fct_order_item', 'order_ts_utc')] if var('enable_watermark_checks', true) else [],
        partition_by={'field': 'order_date', 'data_type': 'date'},
        cluster_by=['order_sk', 'item_key_sk', 'customer_sk', 'is_promo_item']
    )
}}

select
    order_item_sk,
    order_sk,
    customer_sk,
    purchase_key,
    customer_key,
    item_key,
    item_key_sk,
    item_scd_sk,
    promo_key,
    time_order_received_utc as order_ts_utc,
    order_date_utc,
    order_date_berlin as order_date,
    item_count,
    unit_base_price_gross_eur,
    discount_pct_applied,
    unit_discount_amount_gross_eur,
    unit_final_price_gross_eur,
    line_base_amount_gross_eur,
    line_discount_amount_gross_eur,
    line_final_amount_gross_eur,
    is_promo_item,
    item_name_en,
    item_name_de,
    item_name_preferred,
    item_category,
    brand_name,
    vat_rate_pct
from {{ ref('int_wolt_order_items_promoted') }}
{% if is_incremental() %}
where time_order_received_utc >= (
    timestamp_sub(
        {% if var('enable_watermark_checks', true) %}
            {{ watermark_lookup_expr('fct_order_item') }}
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
