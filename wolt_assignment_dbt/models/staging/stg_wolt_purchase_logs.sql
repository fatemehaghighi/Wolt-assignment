with src as (
    select *
    from {{ source('raw', 'wolt_snack_store_purchase_logs') }}
),
typed as (
    select
        purchase_key,
        customer_key,
        -- Canonical event timestamp used for order chronology and downstream time-valid joins.
        safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S Z', time_order_received_utc) as time_order_received_utc,
        cast(safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S Z', time_order_received_utc) as date) as order_date_utc,
        -- Berlin-local business date for assignment-local analysis and promo date-window matching.
        date(safe.parse_timestamp('%Y-%m-%d %H:%M:%E*S Z', time_order_received_utc), 'Europe/Berlin') as order_date_berlin,
        cast(delivery_distance_line_meters as int64) as delivery_distance_line_meters,
        cast(wolt_service_fee as numeric) as wolt_service_fee_eur,
        cast(courier_base_fee as numeric) as courier_base_fee_eur,
        cast(total_basket_value as numeric) as total_basket_value_eur,
        safe.parse_json(item_basket_description) as item_basket_description_json,
        item_basket_description as item_basket_description_raw
    from src
)
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
from typed
