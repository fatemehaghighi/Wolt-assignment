with base as (
    select *
    from {{ ref('rpt_order_item_item_match_audit') }}
)
select
    count(*) as unmatched_order_item_rows,
    count(distinct purchase_key) as affected_purchase_count,
    count(distinct item_key) as affected_item_key_count,
    min(order_ts_utc) as first_unmatched_order_ts_utc,
    max(order_ts_utc) as latest_unmatched_order_ts_utc,
    case
        when count(*) = 0 then 'ok'
        else 'warning_unmatched_items_present'
    end as overall_match_state,
    case
        when count(*) = 0 then 'all_order_items_mapped_to_item_dimensions'
        else 'some_order_items_missing_item_dim_mapping'
    end as overall_match_state_reason,
    coalesce(array_agg(distinct order_item_sk order by order_item_sk limit 200), cast([] as array<string>)) as problematic_order_item_sks,
    coalesce(array_agg(distinct purchase_key order by purchase_key limit 200), cast([] as array<string>)) as problematic_purchase_keys,
    coalesce(array_agg(distinct item_key order by item_key limit 200), cast([] as array<string>)) as problematic_item_keys
from base
