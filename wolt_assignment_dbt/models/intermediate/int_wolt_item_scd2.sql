with base as (
    select
        {{ surrogate_key(["item_key"]) }} as item_key_sk,
        {{ surrogate_key(["item_key", "time_log_created_utc"]) }} as item_scd_key,
        item_key,
        log_item_id,
        time_log_created_utc as valid_from_utc,
        coalesce(
            lead(time_log_created_utc) over (
                partition by item_key
                order by time_log_created_utc
            ),
            timestamp('9999-12-31 23:59:59+00')
        ) as valid_to_utc,
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
    from {{ ref('stg_wolt_item_logs') }}
)
select
    *,
    valid_to_utc = timestamp('9999-12-31 23:59:59+00') as is_current
from base
