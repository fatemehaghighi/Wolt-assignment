{{ config(materialized='table') }}

with order_categories as (
    select distinct
        order_sk,
        item_category
    from {{ ref('fct_order_item') }}
    where item_category is not null
),

pairs as (
    select
        a.item_category as category_a,
        b.item_category as category_b,
        count(distinct a.order_sk) as pair_orders
    from order_categories as a
    inner join order_categories as b
        on a.order_sk = b.order_sk
        and a.item_category < b.item_category
    group by 1, 2
),

singles as (
    select
        item_category,
        count(distinct order_sk) as category_orders
    from order_categories
    group by 1
),

total as (
    select count(distinct order_sk) as total_orders
    from order_categories
),

scored as (
    select
        p.category_a,
        p.category_b,
        p.pair_orders,
        sa.category_orders as category_a_orders,
        sb.category_orders as category_b_orders,
        t.total_orders,
        safe_divide(sa.category_orders, t.total_orders) as category_a_order_penetration,
        safe_divide(sb.category_orders, t.total_orders) as category_b_order_penetration,
        safe_divide(p.pair_orders, t.total_orders) as support,
        safe_divide(p.pair_orders, sa.category_orders) as confidence_a_to_b,
        safe_divide(p.pair_orders, sb.category_orders) as confidence_b_to_a,
        -- Expected overlap if A and B were independent events.
        safe_divide(sa.category_orders, t.total_orders) * safe_divide(sb.category_orders, t.total_orders) as expected_support_independent,
        (
            safe_divide(sa.category_orders, t.total_orders) * safe_divide(sb.category_orders, t.total_orders)
        ) * t.total_orders as expected_pair_orders_independent,
        safe_divide(
            safe_divide(p.pair_orders, t.total_orders),
            safe_divide(sa.category_orders, t.total_orders) * safe_divide(sb.category_orders, t.total_orders)
        ) as lift
    from pairs as p
    inner join singles as sa
        on sa.item_category = p.category_a
    inner join singles as sb
        on sb.item_category = p.category_b
    cross join total as t
)

select
    category_a,
    category_b,
    pair_orders,
    category_a_orders,
    category_b_orders,
    total_orders,
    category_a_order_penetration,
    category_b_order_penetration,
    support,
    expected_support_independent,
    expected_pair_orders_independent,
    (pair_orders - expected_pair_orders_independent) as pair_orders_vs_expected_delta,
    safe_divide(
        pair_orders - expected_pair_orders_independent,
        nullif(expected_pair_orders_independent, 0)
    ) as pair_orders_vs_expected_pct,
    confidence_a_to_b,
    confidence_b_to_a,
    lift,
    case
        when lift >= {{ var('cross_sell_strong_lift_threshold', 1.3) }}
             and support >= {{ var('cross_sell_high_support_threshold', 0.02) }}
            then 'Strong Affinity (Bundle Candidate)'
        when lift >= {{ var('cross_sell_strong_lift_threshold', 1.3) }}
             and support < {{ var('cross_sell_high_support_threshold', 0.02) }}
            then 'Niche Affinity'
        when support >= {{ var('cross_sell_high_support_threshold', 0.02) }}
             and lift >= {{ var('cross_sell_neutral_lift_lower', 0.9) }}
             and lift <= {{ var('cross_sell_neutral_lift_upper', 1.1) }}
            then 'Popularity Effect (Lift ~ 1)'
        when lift < {{ var('cross_sell_neutral_lift_lower', 0.9) }}
            then 'Substitution / Avoidance'
        else 'Monitor'
    end as actionability_bucket,
    case
        when lift >= {{ var('cross_sell_strong_lift_threshold', 1.3) }}
             and support >= {{ var('cross_sell_high_support_threshold', 0.02) }}
            then 1
        when lift >= {{ var('cross_sell_strong_lift_threshold', 1.3) }}
             and support < {{ var('cross_sell_high_support_threshold', 0.02) }}
            then 2
        when support >= {{ var('cross_sell_high_support_threshold', 0.02) }}
             and lift >= {{ var('cross_sell_neutral_lift_lower', 0.9) }}
             and lift <= {{ var('cross_sell_neutral_lift_upper', 1.1) }}
            then 3
        when lift < {{ var('cross_sell_neutral_lift_lower', 0.9) }}
            then 5
        else 4
    end as actionability_rank
from scored
order by actionability_rank asc, lift desc, pair_orders desc
