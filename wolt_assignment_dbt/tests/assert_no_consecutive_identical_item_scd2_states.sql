-- SCD2 should only contain change events.
-- Consecutive versions for the same item_key_sk must not have identical tracked attributes.

with states as (
    select
        item_key_sk,
        valid_from_utc,
        item_scd_sk,
        {{ surrogate_key([
            "item_name_en",
            "item_name_de",
            "item_name_preferred",
            "item_category",
            "brand_name",
            "number_of_units",
            "weight_in_grams",
            "product_base_price_gross_eur",
            "vat_rate_pct"
        ]) }} as attribute_state_hash,
        lag(
            {{ surrogate_key([
                "item_name_en",
                "item_name_de",
                "item_name_preferred",
                "item_category",
                "brand_name",
                "number_of_units",
                "weight_in_grams",
                "product_base_price_gross_eur",
                "vat_rate_pct"
            ]) }}
        ) over (
            partition by item_key_sk
            order by valid_from_utc, item_scd_sk
        ) as previous_attribute_state_hash
    from {{ ref('int_wolt_item_scd2') }}
)
select *
from states
where previous_attribute_state_hash is not null
    and previous_attribute_state_hash = attribute_state_hash

