{{ config(severity='warn') }}

-- Monitoring guard (warning severity):
-- Every priced order-item row is expected to resolve to an item history version at order time.
-- Warning output is used for late-arrival monitoring and healing/backfill operations.
select
    order_item_sk,
    purchase_key,
    item_key,
    time_order_received_utc
from {{ ref('int_wolt_order_items_priced') }}
where item_key_sk is null
    or item_scd_sk is null
