{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='log_item_id',
        on_schema_change='sync_all_columns',
        pre_hook=[ensure_watermark_table()] if var('enable_watermark_checks', true) else [],
        post_hook=(
            [upsert_model_watermark('int_wolt_item_logs_curated', 'time_log_created_utc'), backfill_last_modified_from_log_date()]
            if var('enable_watermark_checks', true)
            else [backfill_last_modified_from_log_date()]
        ),
        partition_by={
            'field': 'time_log_created_utc',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by=['item_key', 'log_item_id']
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
            timestamp_sub(
                {% if var('enable_watermark_checks', true) %}
                    {{ watermark_lookup_expr('int_wolt_item_logs_curated') }}
                {% else %}
                    (
                        select coalesce(
                            max(time_log_created_utc),
                            timestamp('1900-01-01 00:00:00+00')
                        )
                        from {{ this }}
                    )
                {% endif %},
                interval {{ var('incremental_lookback_days', 7) }} day
            )
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
with_merge_status as (
    select
        s.*,
        {% if is_incremental() %}
            case
                when t.log_item_id is null then s.time_log_created_utc
                else current_timestamp()
            end as last_modified_utc
        {% else %}
            s.time_log_created_utc as last_modified_utc
        {% endif %}
    from deduped_best_record as s
    {% if is_incremental() %}
        left join (
            select log_item_id
            from {{ this }}
        ) as t
            on s.log_item_id = t.log_item_id
    {% endif %}
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
    last_modified_utc,
    payload_json,
    payload_raw
from with_merge_status
where product_base_price_gross_eur is not null
    and product_base_price_gross_eur > 0
