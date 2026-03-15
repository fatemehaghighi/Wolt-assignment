-- Row-level audit of item-log curation behavior from raw/staging to curated.
-- One row per log_item_id with explicit expected-vs-actual inclusion status.

with raw_counts as (
    select
        log_item_id,
        count(*) as raw_row_count
    from {{ source('raw', 'wolt_snack_store_item_logs') }}
    group by 1
),
stg as (
    select
        log_item_id,
        item_key,
        time_log_created_utc,
        product_base_price_gross_eur,
        time_item_created_in_source_utc,
        payload_raw
    from {{ ref('stg_wolt_item_logs') }}
),
ranked_by_log_item_id as (
    select
        *,
        row_number() over (
            partition by log_item_id
            order by
                case
                    when product_base_price_gross_eur is not null and product_base_price_gross_eur > 0 then 1
                    else 0
                end desc,
                time_log_created_utc desc,
                payload_raw desc
        ) as preference_rank
    from stg
    where time_log_created_utc is not null
),
best_by_log_item_id as (
    select *
    from ranked_by_log_item_id
    where preference_rank = 1
),
ranked_item_timestamp as (
    select
        *,
        row_number() over (
            partition by item_key, time_log_created_utc
            order by
                case
                    when product_base_price_gross_eur is not null and product_base_price_gross_eur > 0 then 1
                    else 0
                end desc,
                time_item_created_in_source_utc desc nulls last,
                log_item_id desc,
                payload_raw desc
        ) as item_ts_preference_rank
    from best_by_log_item_id
),
stg_by_log as (
    select
        s.log_item_id,
        count(*) as stg_row_count,
        countif(s.time_log_created_utc is null) as stg_rows_with_null_time,
        countif(s.time_log_created_utc is not null) as stg_rows_with_valid_time,
        countif(s.product_base_price_gross_eur is not null and s.product_base_price_gross_eur > 0) as stg_rows_with_positive_price,
        countif(s.product_base_price_gross_eur is null or s.product_base_price_gross_eur <= 0) as stg_rows_with_invalid_price
    from stg as s
    group by 1
),
best_candidate as (
    select
        log_item_id,
        time_log_created_utc as best_candidate_time_log_created_utc,
        product_base_price_gross_eur as best_candidate_price_gross_eur
    from ranked_item_timestamp
    where item_ts_preference_rank = 1
),
expected as (
    select
        b.log_item_id,
        b.stg_row_count,
        b.stg_rows_with_null_time,
        b.stg_rows_with_valid_time,
        b.stg_rows_with_positive_price,
        b.stg_rows_with_invalid_price,
        c.best_candidate_time_log_created_utc,
        c.best_candidate_price_gross_eur,
        case
            when b.stg_rows_with_valid_time = 0 then false
            when c.best_candidate_price_gross_eur is null or c.best_candidate_price_gross_eur <= 0 then false
            else true
        end as expected_in_curated_flag,
        case
            when b.stg_rows_with_valid_time = 0 then 'excluded_all_rows_invalid_time'
            when c.best_candidate_price_gross_eur is null or c.best_candidate_price_gross_eur <= 0 then 'excluded_best_candidate_invalid_price'
            when c.log_item_id is null then 'excluded_by_item_timestamp_conflict_resolution'
            else 'included_best_candidate_positive_price'
        end as expected_reason
    from stg_by_log as b
    left join best_candidate as c
        on b.log_item_id = c.log_item_id
),
curated as (
    select
        log_item_id,
        time_log_created_utc as curated_time_log_created_utc,
        product_base_price_gross_eur as curated_price_gross_eur
    from {{ ref('int_wolt_item_logs_curated_deduped') }}
)
select
    e.log_item_id,
    coalesce(r.raw_row_count, 0) as raw_row_count,
    e.stg_row_count,
    e.stg_rows_with_null_time,
    e.stg_rows_with_valid_time,
    e.stg_rows_with_positive_price,
    e.stg_rows_with_invalid_price,
    e.best_candidate_time_log_created_utc,
    e.best_candidate_price_gross_eur,
    e.expected_in_curated_flag,
    e.expected_reason,
    c.log_item_id is not null as in_curated_flag,
    c.curated_time_log_created_utc,
    c.curated_price_gross_eur,
    case
        when e.expected_in_curated_flag and c.log_item_id is not null then 'ok_expected_and_present'
        when not e.expected_in_curated_flag and c.log_item_id is null then concat('ok_expected_exclusion__', e.expected_reason)
        when e.expected_in_curated_flag and c.log_item_id is null then 'error_expected_but_missing_in_curated'
        else 'error_unexpected_present_in_curated'
    end as curation_consistency_status
from expected as e
left join curated as c
    on e.log_item_id = c.log_item_id
left join raw_counts as r
    on e.log_item_id = r.log_item_id
