select
    item_scd_sk,
    item_key_sk,
    log_item_id,
    valid_from_utc,
    valid_to_utc,
    is_current,
    item_name_en,
    item_name_de,
    item_name_preferred,
    item_category,
    brand_name,
    number_of_units,
    weight_in_grams,
    product_base_price_gross_eur,
    vat_rate_pct,
    time_item_created_in_source_utc
from {{ ref('int_wolt_item_scd2') }}
