{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='log_item_id',
        on_schema_change='sync_all_columns',
        pre_hook=[ensure_watermark_table()] if var('enable_watermark_checks', true) else [],
        post_hook=[
            upsert_model_watermark(
                'int_wolt_item_logs_curated_deduped',
                'time_log_created_utc',
                '`' ~ target.database ~ '`.`' ~ target.schema ~ '_int`.`int_wolt_item_logs_curated_deduped`'
            )
        ] if var('enable_watermark_checks', true) else [],
        partition_by={
            'field': 'time_log_created_utc',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by=['item_key']
    )
}}

-- Purpose:
-- Build a trusted, analytics-ready item-log stream from source-conformed staging logs.
-- This is the layer where quality rules and incremental processing are intentionally applied.
-- Scale design:
-- Default behavior uses a lightweight watermark table to avoid scanning the full target table
-- for max(event_ts) on every run.
-- Note: BigQuery merge join predicates do not allow subqueries, so watermark cutoff is applied on
-- the source side filter (not via incremental_predicates on DBT_INTERNAL_DEST).
-- Toggle:
-- var('enable_watermark_checks', true) can temporarily disable watermark logic and fall back to
-- target-table max(timestamp) cutoff.

with filtered as (
    -- Keep only parseable event-time rows.
    -- In dev: optionally apply deterministic date-window sampling.
    -- In incremental runs: only process recent rows using a configurable lookback window.
    select *
    from {{ ref('stg_wolt_item_logs') }}
    where time_log_created_utc is not null
    {{ dev_date_window('time_log_created_utc', 'timestamp') }}
    {% if is_incremental() %}
        and time_log_created_utc >= (
            {{ incremental_cutoff_expr('int_wolt_item_logs_curated_deduped', 'time_log_created_utc') }}
        )
    {% endif %}
),
deduped_best_record as (
    -- Data quality rule for duplicate log_item_id groups:
    -- 1) Prefer rows with positive non-null price.
    -- 2) If tied, prefer the latest log timestamp.
    -- 3) If still tied, use payload_raw as deterministic tie-breaker.
    select *
    from filtered
    qualify row_number() over (
        partition by log_item_id
        order by
            case
                when product_base_price_gross_eur is not null and product_base_price_gross_eur > 0 then 1
                else 0
            end desc,
            time_log_created_utc desc,
            payload_raw desc
    ) = 1
),
resolved_item_timestamp_conflicts as (
    -- Conflict resolution for same item + same effective log timestamp across different log_item_id values:
    -- 1) Prefer rows with positive non-null price (defensive; price filter is applied again later).
    -- 2) If tied, prefer the latest source-created timestamp when available.
    -- 3) If still tied, use log_item_id then payload_raw as deterministic fallback.
    -- Result: exactly one trusted row per (item_key, time_log_created_utc).
    select *
    from deduped_best_record
    qualify row_number() over (
        partition by item_key, time_log_created_utc
        order by
            case
                when product_base_price_gross_eur is not null and product_base_price_gross_eur > 0 then 1
                else 0
            end desc,
            time_item_created_in_source_utc desc nulls last,
            log_item_id desc,
            payload_raw desc
    ) = 1
)
-- Final business-quality guardrail:
-- keep only rows with positive, non-null product base price.
select
    log_item_id,
    item_key,
    time_log_created_utc,
    item_name_en,
    item_name_de,
    item_name_preferred,
    brand_name,
    item_category,
    number_of_units,
    weight_in_grams,
    product_base_price_gross_eur,
    vat_rate_pct,
    time_item_created_in_source_utc,
    payload_json,
    payload_raw
from resolved_item_timestamp_conflicts
where product_base_price_gross_eur is not null
    and product_base_price_gross_eur > 0
