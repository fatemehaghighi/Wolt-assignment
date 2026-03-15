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
        count(distinct log_item_id) as curated_distinct_log_item_id_count,
        count(distinct item_key) as curated_distinct_item_key_count
    from {{ ref('int_wolt_item_logs_curated_deduped') }}
),
scd2_totals as (
    select
        count(*) as int_item_scd2_row_count,
        count(distinct item_key) as int_item_scd2_distinct_item_key_count
    from {{ ref('int_wolt_item_scd2') }}
),
history_totals as (
    select
        count(*) as dim_item_history_row_count,
        count(distinct item_key) as dim_item_history_distinct_item_key_count
    from {{ ref('dim_item_history') }}
),
current_totals as (
    select
        count(*) as dim_item_current_row_count,
        count(distinct item_key) as dim_item_current_distinct_item_key_count
    from {{ ref('dim_item_current') }}
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
),
problematic_ids as (
    select
        array_agg(distinct log_item_id order by log_item_id limit 200) as problematic_log_item_ids,
        count(distinct log_item_id) as problematic_log_item_id_count
    from {{ ref('rpt_item_logs_curation_audit') }}
    where curation_consistency_status in (
        'error_expected_but_missing_in_curated',
        'error_unexpected_present_in_curated'
    )
),
expected_exclusion_ids as (
    select
        array_agg(distinct log_item_id order by log_item_id limit 200) as expected_exclusion_log_item_ids,
        count(distinct log_item_id) as expected_exclusion_log_item_id_count
    from {{ ref('rpt_item_logs_curation_audit') }}
    where curation_consistency_status like 'ok_expected_exclusion__%'
),
problematic_item_keys as (
    select
        array_agg(distinct s.item_key order by s.item_key limit 200) as problematic_item_keys,
        count(distinct s.item_key) as problematic_item_key_count
    from {{ ref('stg_wolt_item_logs') }} as s
    inner join {{ ref('rpt_item_logs_curation_audit') }} as a
        on s.log_item_id = a.log_item_id
    where a.curation_consistency_status in (
        'error_expected_but_missing_in_curated',
        'error_unexpected_present_in_curated'
    )
)
select
    rt.raw_row_count,
    rt.raw_distinct_log_item_id_count,
    st.stg_row_count,
    st.stg_distinct_log_item_id_count,
    ct.curated_row_count,
    ct.curated_distinct_log_item_id_count,
    ct.curated_distinct_item_key_count,
    s2.int_item_scd2_row_count,
    s2.int_item_scd2_distinct_item_key_count,
    ht.dim_item_history_row_count,
    ht.dim_item_history_distinct_item_key_count,
    cu.dim_item_current_row_count,
    cu.dim_item_current_distinct_item_key_count,
    ac.audit_log_item_id_count,
    ac.expected_in_curated_count,
    ac.actually_in_curated_count,
    ac.excluded_invalid_time_count,
    ac.excluded_invalid_price_count,
    ac.excluded_item_timestamp_conflict_count,
    ac.ok_expected_and_present_count,
    ac.ok_expected_exclusion_count,
    ac.error_expected_but_missing_count,
    ac.error_unexpected_present_count,
    case
        when ac.error_expected_but_missing_count > 0 or ac.error_unexpected_present_count > 0 then 'error'
        when ac.ok_expected_exclusion_count > 0 then 'warning_expected_exclusions_present'
        else 'ok'
    end as overall_curation_state,
    case
        when ac.error_expected_but_missing_count > 0 or ac.error_unexpected_present_count > 0
            then 'curation_mismatch_detected'
        when ac.ok_expected_exclusion_count > 0
            then 'expected_exclusions_present_review_if_unusual'
        else 'all_expected_log_item_ids_present_with_no_mismatch'
    end as overall_curation_state_reason,
    coalesce(pi.problematic_log_item_ids, cast([] as array<string>)) as problematic_log_item_ids,
    coalesce(pk.problematic_item_keys, cast([] as array<string>)) as problematic_item_keys,
    pi.problematic_log_item_id_count,
    pk.problematic_item_key_count,
    coalesce(ex.expected_exclusion_log_item_ids, cast([] as array<string>)) as expected_exclusion_log_item_ids,
    ex.expected_exclusion_log_item_id_count
from raw_totals as rt
cross join stg_totals as st
cross join curated_totals as ct
cross join scd2_totals as s2
cross join history_totals as ht
cross join current_totals as cu
cross join audit_counts as ac
cross join problematic_ids as pi
cross join expected_exclusion_ids as ex
cross join problematic_item_keys as pk
