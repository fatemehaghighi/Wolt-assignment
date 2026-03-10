select
    count(*) as total_steps,
    countif(status = 'matches_expectation') as matching_steps,
    countif(status = 'attention_required') as attention_required_steps,
    countif(status = 'review_required') as review_required_steps,
    countif(status = 'baseline_reference') as baseline_reference_steps,
    max(audit_generated_at_utc) as latest_audit_generated_at_utc
from {{ ref('rpt_pipeline_row_flow_audit') }}
