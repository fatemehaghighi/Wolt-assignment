{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='purchase_key',
        on_schema_change='sync_all_columns',
        pre_hook=[ensure_watermark_table()] if var('enable_watermark_checks', true) else [],
        post_hook=[upsert_model_watermark('int_wolt_purchase_logs_curated', 'time_order_received_utc')] if var('enable_watermark_checks', true) else [],
        partition_by={
            'field': 'order_date_utc',
            'data_type': 'date'
        },
        cluster_by=['customer_key', 'purchase_key']
    )
}}

-- Scale design:
-- Default behavior uses watermark-based incremental cutoff instead of max(timestamp) over the
-- target table.
-- Note: BigQuery merge join predicates do not allow subqueries, so watermark cutoff is applied on
-- the source side filter (not via incremental_predicates on DBT_INTERNAL_DEST).
-- Toggle:
-- var('enable_watermark_checks', true) can temporarily disable watermark logic and fall back to
-- target-table max(timestamp) cutoff.

select
    purchase_key,
    customer_key,
    time_order_received_utc,
    order_date_utc,
    order_date_berlin,
    delivery_distance_line_meters,
    wolt_service_fee_eur,
    courier_base_fee_eur,
    total_basket_value_eur,
    item_basket_description_json,
    item_basket_description_raw
from {{ ref('stg_wolt_purchase_logs') }}
where time_order_received_utc is not null
{{ dev_date_window('time_order_received_utc', 'timestamp') }}
{% if is_incremental() %}
    and time_order_received_utc >= (
        timestamp_sub(
            {% if var('enable_watermark_checks', true) %}
                {{ watermark_lookup_expr('int_wolt_purchase_logs_curated') }}
            {% else %}
                (
                    select coalesce(
                        max(time_order_received_utc),
                        timestamp('1900-01-01 00:00:00+00')
                    )
                    from {{ this }}
                )
            {% endif %},
            interval {{ var('incremental_lookback_days', 7) }} day
        )
    )
{% endif %}
