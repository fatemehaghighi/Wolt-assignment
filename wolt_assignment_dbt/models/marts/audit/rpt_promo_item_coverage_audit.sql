-- Promo-to-item-dimension coverage audit.
-- Shows which promo rows do not map to current item dimension.
select
    p.promo_sk,
    p.item_key_sk,
    p.item_key,
    p.promo_type,
    p.discount_pct,
    p.promo_start_date,
    p.promo_end_date,
    i.item_key_sk is not null as in_dim_item_current_flag,
    case
        when i.item_key_sk is not null then 'ok_mapped'
        else 'warning_missing_item_in_dim_current'
    end as coverage_status
from {{ ref('dim_promo') }} as p
left join {{ ref('dim_item_current') }} as i
    on p.item_key_sk = i.item_key_sk
