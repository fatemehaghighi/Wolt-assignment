-- Business-side promo anomaly monitoring.
-- Non-blocking audit surface to catch unusual discount behavior.

with base as (
    select
        promo_sk,
        item_key,
        promo_type,
        promo_start_date,
        promo_end_date,
        cast(discount_pct as numeric) as discount_pct
    from {{ ref('dim_promo') }}
    where discount_pct is not null
),
item_stats as (
    select
        item_key,
        count(*) as item_obs_count,
        approx_quantiles(discount_pct, 100)[offset(25)] as item_p25_discount_pct,
        approx_quantiles(discount_pct, 100)[offset(50)] as item_p50_discount_pct,
        approx_quantiles(discount_pct, 100)[offset(75)] as item_p75_discount_pct,
        approx_quantiles(discount_pct, 100)[offset(95)] as item_p95_discount_pct
    from base
    group by 1
),
global_stats as (
    select
        count(*) as global_obs_count,
        approx_quantiles(discount_pct, 100)[offset(25)] as global_p25_discount_pct,
        approx_quantiles(discount_pct, 100)[offset(50)] as global_p50_discount_pct,
        approx_quantiles(discount_pct, 100)[offset(75)] as global_p75_discount_pct,
        approx_quantiles(discount_pct, 100)[offset(95)] as global_p95_discount_pct
    from base
),
scored as (
    select
        current_timestamp() as audit_generated_at_utc,
        b.promo_sk,
        b.item_key,
        b.promo_type,
        b.promo_start_date,
        b.promo_end_date,
        b.discount_pct,
        i.item_obs_count,
        i.item_p50_discount_pct,
        i.item_p95_discount_pct,
        i.item_p75_discount_pct - i.item_p25_discount_pct as item_iqr_discount_pct,
        g.global_obs_count,
        g.global_p50_discount_pct,
        g.global_p95_discount_pct,
        g.global_p75_discount_pct - g.global_p25_discount_pct as global_iqr_discount_pct,
        case
            when b.discount_pct < 0 or b.discount_pct > 100 then true
            else false
        end as hard_invalid_discount_flag,
        case
            when b.discount_pct >= 90 then true
            else false
        end as extreme_discount_flag,
        case
            when coalesce(i.item_obs_count, 0) >= 5 then
                b.discount_pct > (
                    coalesce(i.item_p95_discount_pct, 0)
                    + greatest(5, 1.5 * coalesce(i.item_p75_discount_pct - i.item_p25_discount_pct, 0))
                )
            else
                b.discount_pct > (
                    coalesce(g.global_p95_discount_pct, 0)
                    + greatest(5, 1.5 * coalesce(g.global_p75_discount_pct - g.global_p25_discount_pct, 0))
                )
        end as statistically_high_discount_flag
    from base as b
    left join item_stats as i
        on b.item_key = i.item_key
    cross join global_stats as g
)
select
    audit_generated_at_utc,
    promo_sk,
    item_key,
    promo_type,
    promo_start_date,
    promo_end_date,
    discount_pct,
    item_obs_count,
    item_p50_discount_pct,
    item_p95_discount_pct,
    item_iqr_discount_pct,
    global_obs_count,
    global_p50_discount_pct,
    global_p95_discount_pct,
    global_iqr_discount_pct,
    hard_invalid_discount_flag,
    extreme_discount_flag,
    statistically_high_discount_flag,
    case
        when hard_invalid_discount_flag then 'critical'
        when extreme_discount_flag then 'critical'
        when statistically_high_discount_flag then 'warning'
        else 'ok'
    end as anomaly_severity,
    case
        when hard_invalid_discount_flag then 'discount_outside_0_100'
        when extreme_discount_flag then 'discount_at_or_above_90'
        when statistically_high_discount_flag then 'discount_statistical_outlier'
        else 'normal_discount_range'
    end as anomaly_reason
from scored
