{{ config(severity='warn') }}

-- Warning monitor:
-- promo rows that do not map to current item dimension.
select
    promo_sk,
    item_key,
    promo_start_date,
    promo_end_date
from {{ ref('rpt_promo_item_coverage_audit') }}
where not in_dim_item_current_flag
