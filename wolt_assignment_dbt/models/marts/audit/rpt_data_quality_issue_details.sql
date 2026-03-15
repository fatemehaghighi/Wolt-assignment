-- Unified row-level issue surface across audit controls.
-- Each row is a problematic observation with reason and compact context payload.

with item_curation_errors as (
    select
        current_timestamp() as issue_generated_at_utc,
        'item_logs_curation' as issue_domain,
        'critical' as issue_severity,
        curation_consistency_status as issue_reason,
        log_item_id as primary_entity_id,
        cast(null as string) as secondary_entity_id,
        best_candidate_time_log_created_utc as event_ts_utc,
        cast(best_candidate_price_gross_eur as float64) as metric_value,
        to_json_string(struct(
            raw_row_count,
            stg_row_count,
            expected_reason,
            expected_in_curated_flag,
            in_curated_flag
        )) as context_payload
    from {{ ref('rpt_item_logs_curation_audit') }}
    where curation_consistency_status in (
        'error_expected_but_missing_in_curated',
        'error_unexpected_present_in_curated'
    )
),
order_item_missing_item as (
    select
        current_timestamp() as issue_generated_at_utc,
        'order_item_item_mapping' as issue_domain,
        'warning' as issue_severity,
        item_match_status as issue_reason,
        order_item_sk as primary_entity_id,
        item_key as secondary_entity_id,
        order_ts_utc as event_ts_utc,
        cast(order_item_row_final_amount_gross_eur as float64) as metric_value,
        to_json_string(struct(
            purchase_key,
            customer_key,
            item_key_sk,
            item_scd_sk,
            units_in_order_item_row,
            item_unit_base_price_gross_eur
        )) as context_payload
    from {{ ref('rpt_order_item_item_match_audit') }}
),
promo_missing_item as (
    select
        current_timestamp() as issue_generated_at_utc,
        'promo_item_mapping' as issue_domain,
        'warning' as issue_severity,
        coverage_status as issue_reason,
        promo_sk as primary_entity_id,
        item_key as secondary_entity_id,
        cast(promo_start_date as timestamp) as event_ts_utc,
        cast(discount_pct as float64) as metric_value,
        to_json_string(struct(
            promo_type,
            promo_start_date,
            promo_end_date,
            in_dim_item_current_flag
        )) as context_payload
    from {{ ref('rpt_promo_item_coverage_audit') }}
    where not in_dim_item_current_flag
),
promo_discount_anomalies as (
    select
        current_timestamp() as issue_generated_at_utc,
        'promo_discount_anomaly' as issue_domain,
        anomaly_severity as issue_severity,
        anomaly_reason as issue_reason,
        promo_sk as primary_entity_id,
        item_key as secondary_entity_id,
        cast(promo_start_date as timestamp) as event_ts_utc,
        cast(discount_pct as float64) as metric_value,
        to_json_string(struct(
            promo_type,
            promo_start_date,
            promo_end_date,
            item_obs_count,
            item_p95_discount_pct,
            global_p95_discount_pct,
            hard_invalid_discount_flag,
            extreme_discount_flag,
            statistically_high_discount_flag
        )) as context_payload
    from {{ ref('rpt_business_promo_discount_anomaly_audit') }}
    where anomaly_severity != 'ok'
),
item_price_anomalies as (
    select
        current_timestamp() as issue_generated_at_utc,
        'item_price_anomaly' as issue_domain,
        anomaly_severity as issue_severity,
        anomaly_reason as issue_reason,
        log_item_id as primary_entity_id,
        item_key as secondary_entity_id,
        time_log_created_utc as event_ts_utc,
        cast(item_price_gross_eur as float64) as metric_value,
        to_json_string(struct(
            prev_item_price_gross_eur,
            price_ratio_vs_prev,
            item_obs_count,
            item_p95_price,
            invalid_price_flag,
            extreme_jump_flag,
            large_jump_flag,
            statistical_price_outlier_flag
        )) as context_payload
    from {{ ref('rpt_business_item_price_anomaly_audit') }}
    where anomaly_severity != 'ok'
),
pipeline_flow_issues as (
    select
        current_timestamp() as issue_generated_at_utc,
        'pipeline_row_flow' as issue_domain,
        case when status = 'review_required' then 'critical' else 'warning' end as issue_severity,
        status as issue_reason,
        concat(chain_name, '::', object_name) as primary_entity_id,
        cast(step_order as string) as secondary_entity_id,
        audit_generated_at_utc as event_ts_utc,
        cast(row_delta_from_prev as float64) as metric_value,
        to_json_string(struct(
            chain_name,
            object_name,
            primary_grain_key,
            row_count,
            distinct_primary_key_count,
            prev_step_row_count,
            status,
            expected_behavior,
            diagnostic_reason
        )) as context_payload
    from {{ ref('rpt_pipeline_row_flow_audit') }}
    where status in ('review_required', 'attention_required')
),
order_revenue_reconciliation_issues as (
    with raw_base as (
        select
            purchase_key,
            safe_cast(total_basket_value as numeric) as raw_total_basket_value_eur
        from {{ source('raw', 'wolt_snack_store_purchase_logs') }}
    ),
    raw_by_order as (
        select
            purchase_key,
            count(*) as raw_row_count,
            count(distinct raw_total_basket_value_eur) as raw_distinct_total_count,
            max(raw_total_basket_value_eur) as raw_total_basket_value_eur
        from raw_base
        group by 1
    ),
    derived_by_order as (
        select
            o.purchase_key,
            coalesce(sum(oi.order_item_row_final_amount_gross_eur), 0) as derived_basket_value_eur
        from {{ ref('fct_order') }} as o
        left join {{ ref('fct_order_item') }} as oi
            on o.order_sk = oi.order_sk
        group by 1
    )
    select
        current_timestamp() as issue_generated_at_utc,
        'order_revenue_reconciliation' as issue_domain,
        'critical' as issue_severity,
        case
            when r.purchase_key is null then 'missing_raw_purchase_key'
            when r.raw_row_count != 1 then 'duplicate_raw_purchase_key_rows'
            when r.raw_total_basket_value_eur is null then 'raw_total_basket_value_unparsable'
            when abs(d.derived_basket_value_eur - r.raw_total_basket_value_eur) > 0.001 then 'derived_vs_raw_value_mismatch'
            else 'ok'
        end as issue_reason,
        d.purchase_key as primary_entity_id,
        cast(null as string) as secondary_entity_id,
        cast(null as timestamp) as event_ts_utc,
        cast(abs(d.derived_basket_value_eur - coalesce(r.raw_total_basket_value_eur, 0)) as float64) as metric_value,
        to_json_string(struct(
            d.derived_basket_value_eur as derived_basket_value_eur,
            r.raw_total_basket_value_eur as raw_total_basket_value_eur,
            r.raw_row_count as raw_row_count,
            r.raw_distinct_total_count as raw_distinct_total_count
        )) as context_payload
    from derived_by_order as d
    left join raw_by_order as r
        on d.purchase_key = r.purchase_key
    where r.purchase_key is null
        or r.raw_row_count != 1
        or r.raw_total_basket_value_eur is null
        or abs(d.derived_basket_value_eur - r.raw_total_basket_value_eur) > 0.001
)
select * from item_curation_errors
union all
select * from order_item_missing_item
union all
select * from promo_missing_item
union all
select * from promo_discount_anomalies
union all
select * from item_price_anomalies
union all
select * from pipeline_flow_issues
union all
select * from order_revenue_reconciliation_issues
