-- End-to-end row-flow audit across major objects.
-- Goal: make row-count changes explicit by chain/grain and surface expected vs unexpected deltas.

with counts as (
    select
        (select count(*) from {{ source('raw', 'wolt_snack_store_purchase_logs') }}) as raw_purchase_rows,
        (select count(distinct purchase_key) from {{ source('raw', 'wolt_snack_store_purchase_logs') }}) as raw_purchase_distinct_purchase_key,
        (select count(*) from {{ ref('stg_wolt_purchase_logs') }}) as stg_purchase_rows,
        (select count(distinct purchase_key) from {{ ref('stg_wolt_purchase_logs') }}) as stg_purchase_distinct_purchase_key,
        (select countif(time_order_received_utc is null) from {{ ref('stg_wolt_purchase_logs') }}) as stg_purchase_null_event_time_rows,
        (select count(*) from {{ ref('int_wolt_purchase_logs_curated_filtered') }}) as int_purchase_rows,
        (select count(distinct purchase_key) from {{ ref('int_wolt_purchase_logs_curated_filtered') }}) as int_purchase_distinct_purchase_key,

        (select count(*) from {{ ref('stg_wolt_order_items') }}) as stg_order_item_rows,
        (select count(distinct purchase_key) from {{ ref('stg_wolt_order_items') }}) as stg_order_item_distinct_purchase_key,
        (select count(distinct item_key) from {{ ref('stg_wolt_order_items') }}) as stg_order_item_distinct_item_key,
        (select count(*) from {{ ref('int_wolt_order_items_with_item_price') }}) as int_priced_rows,
        (select countif(item_key_sk is null or item_scd_sk is null) from {{ ref('int_wolt_order_items_with_item_price') }}) as int_priced_unmatched_item_rows,
        (select count(*) from {{ ref('int_wolt_order_items_with_price_then_promo') }}) as int_promoted_rows,
        (select count(*) from {{ ref('fct_order_item') }}) as fct_order_item_rows,
        (select countif(item_key_sk is null or item_scd_sk is null) from {{ ref('fct_order_item') }}) as fct_order_item_unmatched_item_rows,
        (select count(*) from {{ ref('fct_order') }}) as fct_order_rows,

        (select count(*) from {{ source('raw', 'wolt_snack_store_item_logs') }}) as raw_item_log_rows,
        (select count(distinct log_item_id) from {{ source('raw', 'wolt_snack_store_item_logs') }}) as raw_item_log_distinct_log_item_id,
        (select count(distinct item_key) from {{ source('raw', 'wolt_snack_store_item_logs') }}) as raw_item_log_distinct_item_key,
        (select count(*) from {{ ref('stg_wolt_item_logs') }}) as stg_item_log_rows,
        (select count(distinct log_item_id) from {{ ref('stg_wolt_item_logs') }}) as stg_item_log_distinct_log_item_id,
        (select count(distinct item_key) from {{ ref('stg_wolt_item_logs') }}) as stg_item_log_distinct_item_key,
        (select countif(time_log_created_utc is null) from {{ ref('stg_wolt_item_logs') }}) as stg_item_log_null_event_time_rows,
        (select count(*) from {{ ref('int_wolt_item_logs_curated_deduped') }}) as int_item_log_curated_rows,
        (select count(distinct log_item_id) from {{ ref('int_wolt_item_logs_curated_deduped') }}) as int_item_log_curated_distinct_log_item_id,
        (select count(distinct item_key) from {{ ref('int_wolt_item_logs_curated_deduped') }}) as int_item_log_curated_distinct_item_key,
        (select count(*) from {{ ref('int_wolt_item_scd2') }}) as int_item_scd2_rows,
        (select count(distinct item_key) from {{ ref('int_wolt_item_scd2') }}) as int_item_scd2_distinct_item_key,
        (select count(*) from {{ ref('dim_item_history') }}) as dim_item_history_rows,
        (select count(distinct item_key) from {{ ref('dim_item_history') }}) as dim_item_history_distinct_item_key,
        (select count(*) from {{ ref('dim_item_current') }}) as dim_item_current_rows,
        (select count(distinct item_key) from {{ ref('dim_item_current') }}) as dim_item_current_distinct_item_key,
        (select excluded_invalid_time_count from {{ ref('rpt_item_logs_curation_audit_summary') }}) as item_curation_excluded_invalid_time_count,
        (select excluded_invalid_price_count from {{ ref('rpt_item_logs_curation_audit_summary') }}) as item_curation_excluded_invalid_price_count,
        (select excluded_item_timestamp_conflict_count from {{ ref('rpt_item_logs_curation_audit_summary') }}) as item_curation_excluded_item_timestamp_conflict_count,

        (select count(*) from {{ source('raw', 'wolt_snack_store_promos') }}) as raw_promo_rows,
        (select count(distinct item_key) from {{ source('raw', 'wolt_snack_store_promos') }}) as raw_promo_distinct_item_key,
        (select count(*) from {{ ref('stg_wolt_promos') }}) as stg_promo_rows,
        (select count(distinct item_key) from {{ ref('stg_wolt_promos') }}) as stg_promo_distinct_item_key,
        (select count(*) from {{ ref('dim_promo') }}) as dim_promo_rows,
        (select count(distinct item_key) from {{ ref('dim_promo') }}) as dim_promo_distinct_item_key,
        (select count(*) from {{ ref('dim_promo') }} p left join {{ ref('dim_item_current') }} i on p.item_key_sk = i.item_key_sk where i.item_key_sk is null) as dim_promo_missing_item_rows
),
flow as (
    select
        current_timestamp() as audit_generated_at_utc,
        'purchase_chain' as chain_name,
        1 as step_order,
        'raw.wolt_snack_store_purchase_logs' as object_name,
        'purchase_key' as primary_grain_key,
        c.raw_purchase_rows as row_count,
        c.raw_purchase_distinct_purchase_key as distinct_primary_key_count,
        cast(null as int64) as prev_step_row_count,
        cast(null as int64) as row_delta_from_prev,
        'Baseline raw purchase events.' as expected_behavior,
        'baseline_reference' as status,
        cast(null as string) as diagnostic_reason
    from counts c

    union all
    select
        current_timestamp(),
        'purchase_chain',
        2,
        'stg_wolt_purchase_logs',
        'purchase_key',
        c.stg_purchase_rows,
        c.stg_purchase_distinct_purchase_key,
        c.raw_purchase_rows,
        c.stg_purchase_rows - c.raw_purchase_rows,
        'Expected same grain/near-same row count as raw after typing.',
        case when c.stg_purchase_rows = c.raw_purchase_rows then 'matches_expectation' else 'review_required' end,
        concat(
            'stg_null_event_time_rows=', cast(c.stg_purchase_null_event_time_rows as string),
            '; raw_minus_stg=', cast(c.raw_purchase_rows - c.stg_purchase_rows as string)
        )
    from counts c

    union all
    select
        current_timestamp(),
        'purchase_chain',
        3,
        'int_wolt_purchase_logs_curated_filtered',
        'purchase_key',
        c.int_purchase_rows,
        c.int_purchase_distinct_purchase_key,
        c.stg_purchase_rows,
        c.int_purchase_rows - c.stg_purchase_rows,
        'Expected same grain, may drop invalid-event-time rows.',
        case when c.int_purchase_rows = c.stg_purchase_rows then 'matches_expectation' else 'review_required' end,
        concat('potential_dropped_rows=', cast(c.stg_purchase_rows - c.int_purchase_rows as string))
    from counts c

    union all
    select
        current_timestamp(),
        'order_item_chain',
        1,
        'stg_wolt_order_items',
        'purchase_key x item_key',
        c.stg_order_item_rows,
        c.stg_order_item_distinct_purchase_key,
        cast(null as int64),
        cast(null as int64),
        'Expected row expansion vs purchase grain because basket JSON is exploded to item grain.',
        'baseline_reference',
        concat('distinct_item_keys=', cast(c.stg_order_item_distinct_item_key as string))
    from counts c

    union all
    select
        current_timestamp(),
        'order_item_chain',
        2,
        'int_wolt_order_items_with_item_price',
        'purchase_key x item_key',
        c.int_priced_rows,
        c.stg_order_item_distinct_purchase_key,
        c.stg_order_item_rows,
        c.int_priced_rows - c.stg_order_item_rows,
        'Expected same row count after SCD2 time-valid price lookup.',
        case when c.int_priced_rows = c.stg_order_item_rows then 'matches_expectation' else 'review_required' end,
        concat('unmatched_item_rows=', cast(c.int_priced_unmatched_item_rows as string))
    from counts c

    union all
    select
        current_timestamp(),
        'order_item_chain',
        3,
        'int_wolt_order_items_with_price_then_promo',
        'purchase_key x item_key',
        c.int_promoted_rows,
        c.stg_order_item_distinct_purchase_key,
        c.int_priced_rows,
        c.int_promoted_rows - c.int_priced_rows,
        'Expected same row count after promo matching (one row per order_item_sk).',
        case when c.int_promoted_rows = c.int_priced_rows then 'matches_expectation' else 'review_required' end,
        cast(null as string)
    from counts c

    union all
    select
        current_timestamp(),
        'order_item_chain',
        4,
        'fct_order_item',
        'order_item_sk',
        c.fct_order_item_rows,
        c.fct_order_item_rows,
        c.int_promoted_rows,
        c.fct_order_item_rows - c.int_promoted_rows,
        'Expected same row count as promoted intermediate.',
        case when c.fct_order_item_rows = c.int_promoted_rows then 'matches_expectation' else 'review_required' end,
        concat('unmatched_item_rows=', cast(c.fct_order_item_unmatched_item_rows as string))
    from counts c

    union all
    select
        current_timestamp(),
        'order_chain',
        1,
        'fct_order',
        'order_sk',
        c.fct_order_rows,
        c.fct_order_rows,
        c.int_purchase_rows,
        c.fct_order_rows - c.int_purchase_rows,
        'Expected one row per curated purchase.',
        case when c.fct_order_rows = c.int_purchase_rows then 'matches_expectation' else 'review_required' end,
        cast(null as string)
    from counts c

    union all
    select
        current_timestamp(),
        'item_log_chain',
        1,
        'raw.wolt_snack_store_item_logs',
        'log_item_id',
        c.raw_item_log_rows,
        c.raw_item_log_distinct_log_item_id,
        cast(null as int64),
        cast(null as int64),
        'Baseline raw item-log events.',
        'baseline_reference',
        concat('distinct_item_keys=', cast(c.raw_item_log_distinct_item_key as string))
    from counts c

    union all
    select
        current_timestamp(),
        'item_log_chain',
        2,
        'stg_wolt_item_logs',
        'log_item_id',
        c.stg_item_log_rows,
        c.stg_item_log_distinct_log_item_id,
        c.raw_item_log_rows,
        c.stg_item_log_rows - c.raw_item_log_rows,
        'Expected near-same row count after parsing/typing.',
        case when c.stg_item_log_rows = c.raw_item_log_rows then 'matches_expectation' else 'review_required' end,
        concat(
            'stg_null_event_time_rows=', cast(c.stg_item_log_null_event_time_rows as string),
            '; distinct_item_keys_raw=', cast(c.raw_item_log_distinct_item_key as string),
            '; distinct_item_keys_stg=', cast(c.stg_item_log_distinct_item_key as string)
        )
    from counts c

    union all
    select
        current_timestamp(),
        'item_log_chain',
        3,
        'int_wolt_item_logs_curated_deduped',
        'log_item_id',
        c.int_item_log_curated_rows,
        c.int_item_log_curated_distinct_log_item_id,
        c.stg_item_log_rows,
        c.int_item_log_curated_rows - c.stg_item_log_rows,
        'Expected fewer or equal rows after curation (invalid/conflicting rows removed).',
        case when c.int_item_log_curated_rows <= c.stg_item_log_rows then 'matches_expectation' else 'review_required' end,
        concat(
            'curation_removed_rows=', cast(c.stg_item_log_rows - c.int_item_log_curated_rows as string),
            '; distinct_item_keys_curated=', cast(c.int_item_log_curated_distinct_item_key as string),
            '; excluded_invalid_time=', cast(c.item_curation_excluded_invalid_time_count as string),
            '; excluded_invalid_price=', cast(c.item_curation_excluded_invalid_price_count as string),
            '; excluded_item_timestamp_conflict=', cast(c.item_curation_excluded_item_timestamp_conflict_count as string)
        )
    from counts c

    union all
    select
        current_timestamp(),
        'item_dim_chain',
        1,
        'int_wolt_item_scd2',
        'item_scd_sk',
        c.int_item_scd2_rows,
        c.int_item_scd2_rows,
        cast(null as int64),
        cast(null as int64),
        'SCD2 versions derived from curated item logs.',
        'baseline_reference',
        concat('distinct_item_keys_scd2=', cast(c.int_item_scd2_distinct_item_key as string))
    from counts c

    union all
    select
        current_timestamp(),
        'item_dim_chain',
        2,
        'dim_item_history',
        'item_scd_sk',
        c.dim_item_history_rows,
        c.dim_item_history_rows,
        c.int_item_scd2_rows,
        c.dim_item_history_rows - c.int_item_scd2_rows,
        'Expected same row count as SCD2 source.',
        case when c.dim_item_history_rows = c.int_item_scd2_rows then 'matches_expectation' else 'review_required' end,
        concat('distinct_item_keys_history=', cast(c.dim_item_history_distinct_item_key as string))
    from counts c

    union all
    select
        current_timestamp(),
        'item_dim_chain',
        3,
        'dim_item_current',
        'item_key_sk',
        c.dim_item_current_rows,
        c.dim_item_current_rows,
        c.dim_item_history_rows,
        c.dim_item_current_rows - c.dim_item_history_rows,
        'Expected lower row count than history (one current row per item).',
        case when c.dim_item_current_rows <= c.dim_item_history_rows then 'matches_expectation' else 'review_required' end,
        concat(
            'distinct_item_keys_current=', cast(c.dim_item_current_distinct_item_key as string),
            '; distinct_item_keys_raw=', cast(c.raw_item_log_distinct_item_key as string)
        )
    from counts c

    union all
    select
        current_timestamp(),
        'promo_chain',
        1,
        'raw.wolt_snack_store_promos',
        'item_key + promo window',
        c.raw_promo_rows,
        c.raw_promo_distinct_item_key,
        cast(null as int64),
        cast(null as int64),
        'Baseline raw promo rules.',
        'baseline_reference',
        cast(null as string)
    from counts c

    union all
    select
        current_timestamp(),
        'promo_chain',
        2,
        'stg_wolt_promos',
        'item_key + promo window',
        c.stg_promo_rows,
        c.stg_promo_distinct_item_key,
        c.raw_promo_rows,
        c.stg_promo_rows - c.raw_promo_rows,
        'Expected near-same rows after typing/standardization.',
        case when c.stg_promo_rows = c.raw_promo_rows then 'matches_expectation' else 'review_required' end,
        cast(null as string)
    from counts c

    union all
    select
        current_timestamp(),
        'promo_chain',
        3,
        'dim_promo',
        'promo_sk',
        c.dim_promo_rows,
        c.dim_promo_distinct_item_key,
        c.stg_promo_rows,
        c.dim_promo_rows - c.stg_promo_rows,
        'Promo rule rows; should mostly map to current item dimension.',
        case when c.dim_promo_missing_item_rows = 0 then 'matches_expectation' else 'attention_required' end,
        concat(
            'promo_rows_missing_item_dim=', cast(c.dim_promo_missing_item_rows as string),
            '; distinct_item_keys_dim_promo=', cast(c.dim_promo_distinct_item_key as string)
        )
    from counts c
)
select *
from flow
