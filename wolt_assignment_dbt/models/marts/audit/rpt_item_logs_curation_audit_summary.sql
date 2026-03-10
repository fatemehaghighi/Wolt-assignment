-- Summary view of item-log curation coverage and mismatch signals.
-- Designed for quick monitoring and reconciliation checks.

with raw_totals as (
    select
        count(*) as raw_row_count,
        count(distinct log_item_id) as raw_distinct_log_item_id_count
    from {{ source('raw', 'wolt_snack_store_item_logs') }}
),
stg_totals as (
    select
        count(*) as stg_row_count,
        count(distinct log_item_id) as stg_distinct_log_item_id_count
    from {{ ref('stg_wolt_item_logs') }}
),
curated_totals as (
    select
        count(*) as curated_row_count,
        count(distinct log_item_id) as curated_distinct_log_item_id_count
    from {{ ref('int_wolt_item_logs_curated') }}
),
audit_counts as (
    select
        count(*) as audit_log_item_id_count,
        countif(expected_in_curated_flag) as expected_in_curated_count,
        countif(in_curated_flag) as actually_in_curated_count,
        countif(expected_reason = 'excluded_all_rows_invalid_time') as excluded_invalid_time_count,
        countif(expected_reason = 'excluded_best_candidate_invalid_price') as excluded_invalid_price_count,
        countif(expected_reason = 'excluded_by_item_timestamp_conflict_resolution') as excluded_item_timestamp_conflict_count,
        countif(curation_consistency_status = 'ok_expected_and_present') as ok_expected_and_present_count,
        countif(curation_consistency_status like 'ok_expected_exclusion__%') as ok_expected_exclusion_count,
        countif(curation_consistency_status = 'error_expected_but_missing_in_curated') as error_expected_but_missing_count,
        countif(curation_consistency_status = 'error_unexpected_present_in_curated') as error_unexpected_present_count
    from {{ ref('rpt_item_logs_curation_audit') }}
)
select
    rt.raw_row_count,
    rt.raw_distinct_log_item_id_count,
    st.stg_row_count,
    st.stg_distinct_log_item_id_count,
    ct.curated_row_count,
    ct.curated_distinct_log_item_id_count,
    ac.audit_log_item_id_count,
    ac.expected_in_curated_count,
    ac.actually_in_curated_count,
    ac.excluded_invalid_time_count,
    ac.excluded_invalid_price_count,
    ac.excluded_item_timestamp_conflict_count,
    ac.ok_expected_and_present_count,
    ac.ok_expected_exclusion_count,
    ac.error_expected_but_missing_count,
    ac.error_unexpected_present_count
from raw_totals as rt
cross join stg_totals as st
cross join curated_totals as ct
cross join audit_counts as ac
