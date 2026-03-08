select *
from {{ ref('stg_wolt_promos') }}
where promo_end_date <= promo_start_date
   or discount_pct < 0
   or discount_pct > 100
