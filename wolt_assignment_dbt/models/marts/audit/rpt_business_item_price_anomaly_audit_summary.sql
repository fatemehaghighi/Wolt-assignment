-- Summary view for item price anomaly audit.

with base as (
    select *
    from {{ ref('rpt_business_item_price_anomaly_audit') }}
),
critical_rows as (
    select *
    from base
    where anomaly_severity = 'critical'
),
warning_rows as (
    select *
    from base
    where anomaly_severity = 'warning'
)
select
    max(audit_generated_at_utc) as latest_audit_generated_at_utc,
    count(*) as item_log_rows_checked,
    countif(anomaly_severity = 'ok') as ok_rows,
    countif(anomaly_severity = 'warning') as warning_rows,
    countif(anomaly_severity = 'critical') as critical_rows,
    countif(anomaly_reason = 'price_statistical_outlier') as statistical_outlier_rows,
    countif(anomaly_reason = 'large_price_jump_vs_previous_log') as large_jump_rows,
    countif(anomaly_reason = 'extreme_price_jump_vs_previous_log') as extreme_jump_rows,
    countif(anomaly_reason = 'non_positive_or_null_price') as non_positive_or_null_rows,
    case
        when countif(anomaly_severity = 'critical') > 0 then 'error_critical_anomalies_present'
        when countif(anomaly_severity = 'warning') > 0 then 'warning_statistical_anomalies_present'
        else 'ok'
    end as overall_item_price_anomaly_state,
    case
        when countif(anomaly_severity = 'critical') > 0 then 'critical_item_price_anomalies_need_review'
        when countif(anomaly_severity = 'warning') > 0 then 'non_critical_item_price_anomalies_detected'
        else 'no_item_price_anomalies_detected'
    end as overall_item_price_anomaly_state_reason,
    coalesce((select array_agg(distinct log_item_id order by log_item_id limit 200) from critical_rows), cast([] as array<string>)) as critical_log_item_ids,
    coalesce((select array_agg(distinct item_key order by item_key limit 200) from critical_rows), cast([] as array<string>)) as critical_item_keys,
    coalesce((select array_agg(distinct log_item_id order by log_item_id limit 200) from warning_rows), cast([] as array<string>)) as warning_log_item_ids
from base
