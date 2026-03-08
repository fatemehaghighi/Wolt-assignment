with overlaps as (
    select
        a.item_key,
        a.item_scd_key as left_item_scd_key,
        b.item_scd_key as right_item_scd_key
    from {{ ref('dim_item_history') }} as a
    inner join {{ ref('dim_item_history') }} as b
        on a.item_key = b.item_key
        and a.item_scd_key != b.item_scd_key
        and a.valid_from_utc < b.valid_to_utc
        and b.valid_from_utc < a.valid_to_utc
)
select *
from overlaps
