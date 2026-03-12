with base as (
    select *
    from {{ ref('rpt_pipeline_row_flow_audit') }}
)
select
    count(*) as total_steps,
    countif(status = 'matches_expectation') as matching_steps,
    countif(status = 'attention_required') as attention_required_steps,
    countif(status = 'review_required') as review_required_steps,
    countif(status = 'baseline_reference') as baseline_reference_steps,
    max(audit_generated_at_utc) as latest_audit_generated_at_utc,
    case
        when countif(status = 'review_required') > 0 then 'error_review_required_steps_present'
        when countif(status = 'attention_required') > 0 then 'warning_attention_required_steps_present'
        else 'ok'
    end as overall_pipeline_flow_state,
    case
        when countif(status = 'review_required') > 0 then 'one_or_more_steps_have_unexpected_row_flow'
        when countif(status = 'attention_required') > 0 then 'one_or_more_steps_have_known_risk_to_monitor'
        else 'all_steps_match_or_are_baseline_reference'
    end as overall_pipeline_flow_state_reason,
    coalesce(array_agg(
        if(status = 'review_required', concat(chain_name, '::', object_name), null)
        ignore nulls
        order by chain_name, step_order
        limit 200
    ), cast([] as array<string>)) as review_required_step_objects,
    coalesce(array_agg(
        if(status = 'attention_required', concat(chain_name, '::', object_name), null)
        ignore nulls
        order by chain_name, step_order
        limit 200
    ), cast([] as array<string>)) as attention_required_step_objects
from base
