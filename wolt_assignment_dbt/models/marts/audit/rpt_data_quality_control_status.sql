-- Unified one-row-per-control status board for operational monitoring.
-- This does not replace dbt test artifacts; it consolidates modeled control outcomes
-- and key problematic identifiers into a query-friendly table.

with item_curation as (
    select
        current_timestamp() as audit_generated_at_utc,
        'item_logs_curation' as control_name,
        overall_curation_state as status,
        case
            when overall_curation_state = 'error' then 'critical'
            when starts_with(overall_curation_state, 'warning') then 'warning'
            else 'ok'
        end as severity,
        greatest(error_expected_but_missing_count + error_unexpected_present_count, 0) as problematic_row_count,
        overall_curation_state_reason as reason,
        problematic_log_item_ids as sample_primary_ids,
        problematic_item_keys as sample_secondary_ids
    from {{ ref('rpt_item_logs_curation_audit_summary') }}
),
order_item_match as (
    select
        current_timestamp() as audit_generated_at_utc,
        'order_item_item_mapping' as control_name,
        overall_match_state as status,
        case
            when overall_match_state = 'ok' then 'ok'
            else 'warning'
        end as severity,
        unmatched_order_item_rows as problematic_row_count,
        overall_match_state_reason as reason,
        problematic_order_item_sks as sample_primary_ids,
        problematic_item_keys as sample_secondary_ids
    from {{ ref('rpt_order_item_item_match_audit_summary') }}
),
promo_coverage as (
    select
        current_timestamp() as audit_generated_at_utc,
        'promo_item_mapping' as control_name,
        overall_coverage_state as status,
        case
            when overall_coverage_state = 'ok' then 'ok'
            else 'warning'
        end as severity,
        promo_rows_missing_item_dim_count as problematic_row_count,
        overall_coverage_state_reason as reason,
        missing_mapping_promo_sks as sample_primary_ids,
        missing_mapping_item_keys as sample_secondary_ids
    from {{ ref('rpt_promo_item_coverage_audit_summary') }}
),
promo_anomaly as (
    select
        current_timestamp() as audit_generated_at_utc,
        'promo_discount_anomaly' as control_name,
        overall_promo_anomaly_state as status,
        case
            when starts_with(overall_promo_anomaly_state, 'error') then 'critical'
            when starts_with(overall_promo_anomaly_state, 'warning') then 'warning'
            else 'ok'
        end as severity,
        critical_rows + warning_rows as problematic_row_count,
        overall_promo_anomaly_state_reason as reason,
        critical_promo_sks as sample_primary_ids,
        critical_item_keys as sample_secondary_ids
    from {{ ref('rpt_business_promo_discount_anomaly_audit_summary') }}
),
item_price_anomaly as (
    select
        current_timestamp() as audit_generated_at_utc,
        'item_price_anomaly' as control_name,
        overall_item_price_anomaly_state as status,
        case
            when starts_with(overall_item_price_anomaly_state, 'error') then 'critical'
            when starts_with(overall_item_price_anomaly_state, 'warning') then 'warning'
            else 'ok'
        end as severity,
        critical_rows + warning_rows as problematic_row_count,
        overall_item_price_anomaly_state_reason as reason,
        critical_log_item_ids as sample_primary_ids,
        critical_item_keys as sample_secondary_ids
    from {{ ref('rpt_business_item_price_anomaly_audit_summary') }}
),
pipeline_flow as (
    select
        current_timestamp() as audit_generated_at_utc,
        'pipeline_row_flow' as control_name,
        overall_pipeline_flow_state as status,
        case
            when starts_with(overall_pipeline_flow_state, 'error') then 'critical'
            when starts_with(overall_pipeline_flow_state, 'warning') then 'warning'
            else 'ok'
        end as severity,
        review_required_steps + attention_required_steps as problematic_row_count,
        overall_pipeline_flow_state_reason as reason,
        review_required_step_objects as sample_primary_ids,
        attention_required_step_objects as sample_secondary_ids
    from {{ ref('rpt_pipeline_row_flow_audit_summary') }}
),
order_revenue_reconciliation as (
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
    ),
    failures as (
        select
            d.purchase_key
        from derived_by_order as d
        left join raw_by_order as r
            on d.purchase_key = r.purchase_key
        where r.purchase_key is null
            or r.raw_row_count != 1
            or r.raw_total_basket_value_eur is null
            or abs(d.derived_basket_value_eur - r.raw_total_basket_value_eur) > 0.001
    )
    select
        current_timestamp() as audit_generated_at_utc,
        'order_revenue_reconciliation' as control_name,
        case when count(*) = 0 then 'ok' else 'error_order_revenue_reconciliation_mismatch' end as status,
        case when count(*) = 0 then 'ok' else 'critical' end as severity,
        count(*) as problematic_row_count,
        case when count(*) = 0
            then 'all_orders_reconcile_between_derived_and_raw_totals'
            else 'one_or_more_orders_do_not_reconcile_between_derived_and_raw_totals'
        end as reason,
        coalesce(array_agg(purchase_key order by purchase_key limit 200), cast([] as array<string>)) as sample_primary_ids,
        cast([] as array<string>) as sample_secondary_ids
    from failures
),
control_base as (
    select * from item_curation
    union all
    select * from order_item_match
    union all
    select * from promo_coverage
    union all
    select * from promo_anomaly
    union all
    select * from item_price_anomaly
    union all
    select * from pipeline_flow
    union all
    select * from order_revenue_reconciliation
),
issue_rollup as (
    select
        issue_domain as control_name,
        count(*) as failing_issue_count,
        coalesce(array_agg(
            to_json_string(struct(
                issue_severity,
                issue_reason,
                primary_entity_id,
                secondary_entity_id,
                event_ts_utc,
                metric_value,
                context_payload
            ))
            order by issue_severity desc, issue_reason, primary_entity_id
            limit 200
        ), cast([] as array<string>)) as failing_rows_with_reason
    from {{ ref('rpt_data_quality_issue_details') }}
    group by 1
)
select
    b.audit_generated_at_utc,
    b.control_name,
    case b.control_name
        when 'item_logs_curation' then 'Checks curated item-log consistency vs expected inclusion/exclusion rules.'
        when 'order_item_item_mapping' then 'Checks order-item rows resolve to item entity/version keys.'
        when 'promo_item_mapping' then 'Checks promo rules map to existing item keys in current item dimension.'
        when 'promo_discount_anomaly' then 'Checks promo discounts for critical or statistical anomalies.'
        when 'item_price_anomaly' then 'Checks item-price history for invalid values and unusual jumps/outliers.'
        when 'pipeline_row_flow' then 'Checks row-flow deltas by chain/layer against expected behavior.'
        when 'order_revenue_reconciliation' then 'Checks each order derived basket total equals raw purchase basket total.'
        else 'Consolidated quality control.'
    end as control_description,
    b.status,
    b.severity,
    b.problematic_row_count,
    b.reason,
    coalesce(i.failing_issue_count, 0) as failing_issue_count,
    coalesce(i.failing_rows_with_reason, cast([] as array<string>)) as failing_rows_with_reason,
    b.sample_primary_ids,
    b.sample_secondary_ids
from control_base as b
left join issue_rollup as i
    on b.control_name = i.control_name
