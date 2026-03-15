{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_item_sk',
        on_schema_change='sync_all_columns',
        pre_hook=[ensure_watermark_table()] if var('enable_watermark_checks', true) else [],
        post_hook=[
            upsert_model_watermark(
                'fct_order_item',
                'order_ts_utc',
                '`' ~ target.database ~ '`.`' ~ target.schema ~ '_core`.`fct_order_item`'
            )
        ] if var('enable_watermark_checks', true) else [],
        partition_by={'field': 'order_date', 'data_type': 'date'},
        cluster_by=['order_sk', 'item_key_sk', 'customer_sk']
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
    promo_sk,
    time_order_received_utc as order_ts_utc,
    order_date_utc,
    -- Keep Berlin local date as canonical fact date for assignment-aligned business analysis.
    order_date_berlin as order_date,
    units_in_order_item_row,
    item_unit_base_price_gross_eur,
    discount_pct_applied,
    item_unit_discount_amount_gross_eur,
    item_unit_final_price_gross_eur,
    order_item_row_base_amount_gross_eur,
    order_item_row_discount_amount_gross_eur,
    order_item_row_final_amount_gross_eur,
    is_promo_item,
    item_name_en,
    item_name_de,
    item_name_preferred,
    item_category,
    brand_name,
    vat_rate_pct
from {{ ref('int_wolt_order_items_with_price_then_promo') }}
{% if is_incremental() %}
where time_order_received_utc >= (
    {{ incremental_cutoff_expr('fct_order_item', 'order_ts_utc') }}
)
{% endif %}
