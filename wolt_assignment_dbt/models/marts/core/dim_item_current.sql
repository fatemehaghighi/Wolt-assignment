{{
    config(
        cluster_by=['item_key_sk', 'item_key']
    )
}}

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
