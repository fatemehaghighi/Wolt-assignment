with overlaps as (
    select
        a.item_key_sk,
        a.item_scd_sk as left_item_scd_sk,
        b.item_scd_sk as right_item_scd_sk
    from {{ ref('dim_item_history') }} as a
    inner join {{ ref('dim_item_history') }} as b
        on a.item_key_sk = b.item_key_sk
        and a.item_scd_sk != b.item_scd_sk
        and a.valid_from_utc < b.valid_to_utc
        and b.valid_from_utc < a.valid_to_utc
)
select *
from overlaps
