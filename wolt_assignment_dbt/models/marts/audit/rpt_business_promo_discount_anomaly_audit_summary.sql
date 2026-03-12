-- Summary view for promo discount anomaly audit.

select
    max(audit_generated_at_utc) as latest_audit_generated_at_utc,
    count(*) as promo_rows_checked,
    countif(anomaly_severity = 'ok') as ok_rows,
    countif(anomaly_severity = 'warning') as warning_rows,
    countif(anomaly_severity = 'critical') as critical_rows,
    countif(anomaly_reason = 'discount_statistical_outlier') as statistical_outlier_rows,
    countif(anomaly_reason = 'discount_at_or_above_90') as extreme_discount_rows,
    countif(anomaly_reason = 'discount_outside_0_100') as hard_invalid_discount_rows
from {{ ref('rpt_business_promo_discount_anomaly_audit') }}
