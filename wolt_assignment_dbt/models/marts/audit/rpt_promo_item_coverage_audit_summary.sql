with base as (
    select *
    from {{ ref('rpt_promo_item_coverage_audit') }}
),
missing as (
    select *
    from base
    where not in_dim_item_current_flag
)
select
    count(*) as promo_row_count,
    countif(in_dim_item_current_flag) as promo_rows_mapped_to_item_dim_count,
    countif(not in_dim_item_current_flag) as promo_rows_missing_item_dim_count,
    case
        when countif(not in_dim_item_current_flag) = 0 then 'ok'
        else 'warning_missing_item_mapping'
    end as overall_coverage_state,
    case
        when countif(not in_dim_item_current_flag) = 0 then 'all_promos_mapped_to_current_item_dim'
        else 'some_promos_missing_current_item_dim_mapping'
    end as overall_coverage_state_reason,
    coalesce((select array_agg(distinct promo_sk order by promo_sk limit 200) from missing), cast([] as array<string>)) as missing_mapping_promo_sks,
    coalesce((select array_agg(distinct item_key order by item_key limit 200) from missing), cast([] as array<string>)) as missing_mapping_item_keys
from base
