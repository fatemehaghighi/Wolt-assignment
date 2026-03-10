{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='item_key_sk',
        on_schema_change='sync_all_columns',
        pre_hook=[ensure_watermark_table()] if var('enable_watermark_checks', true) else [],
        post_hook=[
            upsert_model_watermark(
                'dim_item_current',
                'current_valid_from_utc',
                '`' ~ target.database ~ '`.`' ~ target.schema ~ '_core`.`dim_item_current`'
            )
        ] if var('enable_watermark_checks', true) else [],
        cluster_by=['item_key_sk']
    )
}}

with current_items as (
    select
        item_key,
        item_key_sk,
        item_scd_sk,
        item_name_en,
        item_name_de,
        item_name_preferred,
        item_category,
        brand_name,
        number_of_units,
        weight_in_grams,
        product_base_price_gross_eur as current_product_base_price_gross_eur,
        vat_rate_pct as current_vat_rate_pct,
        valid_from_utc as current_valid_from_utc
    from {{ ref('dim_item_history') }}
    where is_current
),
affected_items as (
    {% if is_incremental() %}
        select distinct item_key_sk
        from {{ ref('dim_item_history') }}
        where valid_from_utc >= (
            {{ incremental_cutoff_expr('dim_item_current', 'current_valid_from_utc') }}
        )
    {% else %}
        select distinct item_key_sk
        from current_items
    {% endif %}
)
select c.*
from current_items as c
inner join affected_items as a
    on c.item_key_sk = a.item_key_sk
