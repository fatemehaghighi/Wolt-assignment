with src as (
    select *
    from {{ source('raw', 'wolt_snack_store_promos') }}
)
select
    item_key,
    safe.parse_date('%Y-%m-%d', promo_start_date) as promo_start_date,
    safe.parse_date('%Y-%m-%d', promo_end_date) as promo_end_date,
    promo_type,
    cast(discount_in_percentage as int64) as discount_pct
from src
