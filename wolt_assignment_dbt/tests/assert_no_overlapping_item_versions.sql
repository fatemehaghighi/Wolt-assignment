with ordered_versions as (
    select
        item_key_sk,
        item_scd_sk,
        valid_from_utc,
        valid_to_utc,
        lead(valid_from_utc) over (
            partition by item_key_sk
            order by valid_from_utc, item_scd_sk
        ) as next_valid_from_utc
    from {{ ref('dim_item_history') }}
)
select *
from ordered_versions
where next_valid_from_utc is not null
    and valid_to_utc > next_valid_from_utc
