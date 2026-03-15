-- Business-side item price anomaly monitoring on curated item-log history.
-- Non-blocking controls to detect suspicious price behavior over time.

with base as (
    select
        log_item_id,
        item_key,
        time_log_created_utc,
        cast(product_base_price_gross_eur as numeric) as item_price_gross_eur
    from {{ ref('int_wolt_item_logs_curated_deduped') }}
    where time_log_created_utc is not null
),
item_stats as (
    select
        item_key,
        count(*) as item_obs_count,
        approx_quantiles(item_price_gross_eur, 100)[offset(25)] as item_p25_price,
        approx_quantiles(item_price_gross_eur, 100)[offset(50)] as item_p50_price,
        approx_quantiles(item_price_gross_eur, 100)[offset(75)] as item_p75_price,
        approx_quantiles(item_price_gross_eur, 100)[offset(95)] as item_p95_price
    from base
    group by 1
),
with_prev as (
    select
        b.*,
        lag(item_price_gross_eur) over (
            partition by item_key
            order by time_log_created_utc, log_item_id
        ) as prev_item_price_gross_eur
    from base as b
),
scored as (
    select
        current_timestamp() as audit_generated_at_utc,
        p.log_item_id,
        p.item_key,
        p.time_log_created_utc,
        p.item_price_gross_eur,
        p.prev_item_price_gross_eur,
        i.item_obs_count,
        i.item_p25_price,
        i.item_p50_price,
        i.item_p75_price,
        i.item_p95_price,
        i.item_p75_price - i.item_p25_price as item_iqr_price,
        safe_divide(p.item_price_gross_eur, nullif(p.prev_item_price_gross_eur, 0)) as price_ratio_vs_prev,
        case
            when p.item_price_gross_eur is null or p.item_price_gross_eur <= 0 then true
            else false
        end as invalid_price_flag,
        case
            when p.prev_item_price_gross_eur is null then false
            when safe_divide(p.item_price_gross_eur, nullif(p.prev_item_price_gross_eur, 0)) >= 5 then true
            when safe_divide(p.item_price_gross_eur, nullif(p.prev_item_price_gross_eur, 0)) <= 0.2 then true
            else false
        end as extreme_jump_flag,
        case
            when p.prev_item_price_gross_eur is null then false
            when safe_divide(p.item_price_gross_eur, nullif(p.prev_item_price_gross_eur, 0)) >= 3 then true
            when safe_divide(p.item_price_gross_eur, nullif(p.prev_item_price_gross_eur, 0)) <= 0.33 then true
            else false
        end as large_jump_flag,
        case
            when coalesce(i.item_obs_count, 0) < 8 then false
            when p.item_price_gross_eur > (
                i.item_p95_price + greatest(0.5, 1.5 * (i.item_p75_price - i.item_p25_price))
            ) then true
            when p.item_price_gross_eur < greatest(
                0,
                i.item_p25_price - 1.5 * (i.item_p75_price - i.item_p25_price)
            ) then true
            else false
        end as statistical_price_outlier_flag
    from with_prev as p
    left join item_stats as i
        on p.item_key = i.item_key
)
select
    audit_generated_at_utc,
    log_item_id,
    item_key,
    time_log_created_utc,
    item_price_gross_eur,
    prev_item_price_gross_eur,
    item_obs_count,
    item_p25_price,
    item_p50_price,
    item_p75_price,
    item_p95_price,
    item_iqr_price,
    price_ratio_vs_prev,
    invalid_price_flag,
    extreme_jump_flag,
    large_jump_flag,
    statistical_price_outlier_flag,
    case
        when invalid_price_flag then 'critical'
        when extreme_jump_flag then 'critical'
        when large_jump_flag or statistical_price_outlier_flag then 'warning'
        else 'ok'
    end as anomaly_severity,
    case
        when invalid_price_flag then 'non_positive_or_null_price'
        when extreme_jump_flag then 'extreme_price_jump_vs_previous_log'
        when large_jump_flag then 'large_price_jump_vs_previous_log'
        when statistical_price_outlier_flag then 'price_statistical_outlier'
        else 'normal_price_pattern'
    end as anomaly_reason
from scored
