-- Summary view for promo discount anomaly audit.

with base as (
    select *
    from {{ ref('rpt_business_promo_discount_anomaly_audit') }}
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
    count(*) as promo_rows_checked,
    countif(anomaly_severity = 'ok') as ok_rows,
    countif(anomaly_severity = 'warning') as warning_rows,
    countif(anomaly_severity = 'critical') as critical_rows,
    countif(anomaly_reason = 'discount_statistical_outlier') as statistical_outlier_rows,
    countif(anomaly_reason = 'discount_at_or_above_90') as extreme_discount_rows,
    countif(anomaly_reason = 'discount_outside_0_100') as hard_invalid_discount_rows,
    case
        when countif(anomaly_severity = 'critical') > 0 then 'error_critical_anomalies_present'
        when countif(anomaly_severity = 'warning') > 0 then 'warning_statistical_anomalies_present'
        else 'ok'
    end as overall_promo_anomaly_state,
    case
        when countif(anomaly_severity = 'critical') > 0 then 'critical_promo_discount_anomalies_need_review'
        when countif(anomaly_severity = 'warning') > 0 then 'non_critical_promo_discount_anomalies_detected'
        else 'no_promo_discount_anomalies_detected'
    end as overall_promo_anomaly_state_reason,
    coalesce((select array_agg(distinct promo_sk order by promo_sk limit 200) from critical_rows), cast([] as array<string>)) as critical_promo_sks,
    coalesce((select array_agg(distinct item_key order by item_key limit 200) from critical_rows), cast([] as array<string>)) as critical_item_keys,
    coalesce((select array_agg(distinct promo_sk order by promo_sk limit 200) from warning_rows), cast([] as array<string>)) as warning_promo_sks
from base
