with duplicated as (
    select
        item_key,
        promo_start_date,
        promo_end_date,
        promo_type,
        discount_pct,
        count(*) as row_count
    from {{ ref('stg_wolt_promos') }}
    group by 1, 2, 3, 4, 5
    having count(*) > 1
)
select *
from duplicated
