{{ config(severity='warn') }}

-- Non-blocking business control:
-- emit warning if critical item price anomalies are present.

select
    log_item_id,
    item_key,
    time_log_created_utc,
    item_price_gross_eur,
    prev_item_price_gross_eur,
    price_ratio_vs_prev,
    anomaly_reason
from {{ ref('rpt_business_item_price_anomaly_audit') }}
where anomaly_severity = 'critical'
