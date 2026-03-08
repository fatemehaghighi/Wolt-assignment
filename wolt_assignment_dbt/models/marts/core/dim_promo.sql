select
    {{ surrogate_key(["item_key", "promo_start_date", "promo_end_date", "promo_type", "discount_pct"]) }} as promo_key,
    {{ surrogate_key(["item_key"]) }} as item_key_sk,
    promo_type,
    discount_pct,
    promo_start_date,
    promo_end_date
from {{ ref('stg_wolt_promos') }}
