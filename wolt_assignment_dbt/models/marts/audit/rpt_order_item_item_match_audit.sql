-- Late-arrival / missing-item audit surface:
-- keep process running, but expose exactly which order-items could not resolve to item dims.
select
    order_item_sk,
    order_sk,
    purchase_key,
    customer_sk,
    customer_key,
    order_ts_utc,
    order_date,
    item_key,
    item_key_sk,
    item_scd_sk,
    units_in_order_item_row,
    item_unit_base_price_gross_eur,
    order_item_row_final_amount_gross_eur,
    case
        when item_key_sk is null and item_scd_sk is null then 'missing_item_entity_and_version'
        when item_key_sk is null then 'missing_item_entity'
        when item_scd_sk is null then 'missing_item_version'
        else 'matched'
    end as item_match_status
from {{ ref('fct_order_item') }}
where item_key_sk is null
    or item_scd_sk is null
