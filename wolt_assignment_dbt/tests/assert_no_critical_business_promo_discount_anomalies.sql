{{ config(severity='warn') }}

-- Non-blocking business control:
-- emit warning if critical promo discount anomalies are present.

select
    promo_sk,
    item_key,
    promo_start_date,
    promo_end_date,
    discount_pct,
    anomaly_reason
from {{ ref('rpt_business_promo_discount_anomaly_audit') }}
where anomaly_severity = 'critical'
