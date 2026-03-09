with promos as (
    select
        item_key,
        promo_start_date,
        promo_end_date,
        promo_type,
        discount_pct,
        row_number() over (
            partition by item_key
            order by promo_start_date, promo_end_date, promo_type, discount_pct
        ) as promo_rn
    from {{ ref('stg_wolt_promos') }}
),
overlaps as (
    select
        a.item_key,
        a.promo_start_date as left_start_date,
        a.promo_end_date as left_end_date,
        b.promo_start_date as right_start_date,
        b.promo_end_date as right_end_date
    from promos as a
    inner join promos as b
        on a.item_key = b.item_key
        and a.promo_rn < b.promo_rn
        and a.promo_start_date < b.promo_end_date
        and b.promo_start_date < a.promo_end_date
)
select *
from overlaps
