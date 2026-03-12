-- Summary view for item price anomaly audit.

select
    max(audit_generated_at_utc) as latest_audit_generated_at_utc,
    count(*) as item_log_rows_checked,
    countif(anomaly_severity = 'ok') as ok_rows,
    countif(anomaly_severity = 'warning') as warning_rows,
    countif(anomaly_severity = 'critical') as critical_rows,
    countif(anomaly_reason = 'price_statistical_outlier') as statistical_outlier_rows,
    countif(anomaly_reason = 'large_price_jump_vs_previous_log') as large_jump_rows,
    countif(anomaly_reason = 'extreme_price_jump_vs_previous_log') as extreme_jump_rows,
    countif(anomaly_reason = 'non_positive_or_null_price') as non_positive_or_null_rows
from {{ ref('rpt_business_item_price_anomaly_audit') }}
