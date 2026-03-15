-- Purpose:
-- Build item SCD2 validity windows from curated item log events.
--
-- Key assumptions:
-- 1) int_wolt_item_logs_curated_deduped contains the trusted event stream.
-- 2) time_log_created_utc is the business-effective ordering field.
-- 3) For identical timestamps within an item_key, log_item_id is used as deterministic tie-breaker.
-- 4) No-op republished events can exist with different log_item_id/time but identical item attributes.
--    Example observed:
--    - bcb18b1cff8a79f511e1d7afe55dbda6 (2022-12-30)
--    - 1ef4ee87a85529fcc2f0c88b57277f27 (2023-12-22)
--    Both for item_key 2f605027e796b8c1d897c6100903c6d7 with identical business attributes.
--    SCD2 should keep only true attribute changes, not no-op republish events.
--
-- Operational note:
-- SCD2 correctness depends on upstream curated completeness. If very-late historical events are not
-- included upstream (for example, arriving older than incremental lookback), valid_from/valid_to can
-- remain stale until a wider backfill/full-refresh is run.

with ordered_events as (
    select
        {{ surrogate_key(["item_key"]) }} as item_key_sk,
        item_key,
        log_item_id,
        time_log_created_utc,
        item_name_en,
        item_name_de,
        item_name_preferred,
        item_category,
        brand_name,
        number_of_units,
        weight_in_grams,
        product_base_price_gross_eur,
        vat_rate_pct,
        time_item_created_in_source_utc,
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
            partition by item_key
            order by time_log_created_utc, log_item_id
        ) as previous_attribute_state_hash
    from {{ ref('int_wolt_item_logs_curated_deduped') }}
),
change_events as (
    select *
    from ordered_events
    where previous_attribute_state_hash is null
        or previous_attribute_state_hash != attribute_state_hash
),
base as (
    select
        {{ surrogate_key(["item_key", "time_log_created_utc", "log_item_id"]) }} as item_scd_sk,
        item_key_sk,
        item_key,
        log_item_id,
        time_log_created_utc as valid_from_utc,
        coalesce(
            lead(time_log_created_utc) over (
                partition by item_key
                order by time_log_created_utc, log_item_id
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
    from change_events
)
select
    *,
    valid_to_utc = timestamp('9999-12-31 23:59:59+00') as is_current
from base
