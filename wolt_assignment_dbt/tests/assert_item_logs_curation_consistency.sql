-- Fail if curated inclusion diverges from expected curation logic.

select *
from {{ ref('rpt_item_logs_curation_audit') }}
where curation_consistency_status in (
    'error_expected_but_missing_in_curated',
    'error_unexpected_present_in_curated'
)

