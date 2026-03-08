-- Purpose:
-- Build item SCD2 validity windows from curated item log events.
--
-- Key assumptions:
-- 1) int_wolt_item_logs_curated contains the trusted event stream.
-- 2) time_log_created_utc is the business-effective ordering field.
-- 3) For identical timestamps within an item_key, log_item_id is used as deterministic tie-breaker.
--
-- Operational note:
-- SCD2 correctness depends on upstream curated completeness. If very-late historical events are not
-- included upstream (for example, arriving older than incremental lookback), valid_from/valid_to can
-- remain stale until a wider backfill/full-refresh is run.

with base as (
    select
        {{ surrogate_key(["item_key", "time_log_created_utc", "log_item_id"]) }} as item_scd_sk,
        {{ surrogate_key(["item_key"]) }} as item_key_sk,
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
    from {{ ref('int_wolt_item_logs_curated') }}
)
select
    *,
    valid_to_utc = timestamp('9999-12-31 23:59:59+00') as is_current
from base
