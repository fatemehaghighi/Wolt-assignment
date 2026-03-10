select
    count(*) as unmatched_order_item_rows,
    count(distinct purchase_key) as affected_purchase_count,
    count(distinct item_key) as affected_item_key_count,
    min(order_ts_utc) as first_unmatched_order_ts_utc,
    max(order_ts_utc) as latest_unmatched_order_ts_utc
from {{ ref('rpt_order_item_item_match_audit') }}
