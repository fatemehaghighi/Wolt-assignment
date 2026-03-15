{{ config(materialized='table') }}

with product_revenue as (
    select
        coalesce(item_name_preferred, item_name_en, item_name_de) as product_name,
        sum(order_item_row_final_amount_gross_eur) as revenue_eur
    from {{ ref('fct_order_item') }}
    where coalesce(item_name_preferred, item_name_en, item_name_de) is not null
    group by 1
),

top_products as (
    select product_name
    from product_revenue
    order by revenue_eur desc
    limit {{ var('cross_sell_top_products_limit', 30) }}
),

product_category_revenue as (
    select
        coalesce(item_name_preferred, item_name_en, item_name_de) as product_name,
        item_category,
        sum(order_item_row_final_amount_gross_eur) as category_revenue_eur
    from {{ ref('fct_order_item') }}
    where coalesce(item_name_preferred, item_name_en, item_name_de) is not null
      and item_category is not null
    group by 1, 2
),

product_category_map as (
    select
        product_name,
        item_category
    from (
        select
            product_name,
            item_category,
            category_revenue_eur,
            row_number() over (
                partition by product_name
                order by category_revenue_eur desc, item_category
            ) as rn
        from product_category_revenue
    )
    where rn = 1
),

order_products as (
    select distinct
        f.order_sk,
        coalesce(f.item_name_preferred, f.item_name_en, f.item_name_de) as product_name
    from {{ ref('fct_order_item') }} as f
    inner join top_products as tp
        on coalesce(f.item_name_preferred, f.item_name_en, f.item_name_de) = tp.product_name
    where coalesce(f.item_name_preferred, f.item_name_en, f.item_name_de) is not null
),

pairs as (
    select
        a.product_name as product_a,
        b.product_name as product_b,
        count(distinct a.order_sk) as pair_orders
    from order_products as a
    inner join order_products as b
        on a.order_sk = b.order_sk
        and a.product_name < b.product_name
    group by 1, 2
),

singles as (
    select
        product_name,
        count(distinct order_sk) as product_orders
    from order_products
    group by 1
),

total as (
    select count(distinct order_sk) as total_orders
    from {{ ref('fct_order_item') }}
),

scored as (
    select
        p.product_a,
        p.product_b,
        p.pair_orders,
        sa.product_orders as product_a_orders,
        sb.product_orders as product_b_orders,
        t.total_orders,
        safe_divide(sa.product_orders, t.total_orders) as product_a_order_penetration,
        safe_divide(sb.product_orders, t.total_orders) as product_b_order_penetration,
        safe_divide(p.pair_orders, t.total_orders) as support,
        safe_divide(p.pair_orders, sa.product_orders) as confidence_a_to_b,
        safe_divide(p.pair_orders, sb.product_orders) as confidence_b_to_a,
        safe_divide(sa.product_orders, t.total_orders) * safe_divide(sb.product_orders, t.total_orders) as expected_support_independent,
        (
            safe_divide(sa.product_orders, t.total_orders) * safe_divide(sb.product_orders, t.total_orders)
        ) * t.total_orders as expected_pair_orders_independent,
        safe_divide(
            safe_divide(p.pair_orders, t.total_orders),
            safe_divide(sa.product_orders, t.total_orders) * safe_divide(sb.product_orders, t.total_orders)
        ) as lift
    from pairs as p
    inner join singles as sa on sa.product_name = p.product_a
    inner join singles as sb on sb.product_name = p.product_b
    cross join total as t
    where p.pair_orders >= {{ var('cross_sell_min_pair_orders', 50) }}
)

select
    s.product_a,
    s.product_b,
    s.pair_orders,
    s.product_a_orders,
    s.product_b_orders,
    s.total_orders,
    s.product_a_order_penetration,
    s.product_b_order_penetration,
    s.support,
    s.expected_support_independent,
    s.expected_pair_orders_independent,
    (s.pair_orders - s.expected_pair_orders_independent) as pair_orders_vs_expected_delta,
    safe_divide(
        s.pair_orders - s.expected_pair_orders_independent,
        nullif(s.expected_pair_orders_independent, 0)
    ) as pair_orders_vs_expected_pct,
    s.confidence_a_to_b,
    s.confidence_b_to_a,
    s.lift,
    pca.item_category as category_a,
    pcb.item_category as category_b,
    case
        when pca.item_category = 'Crisp & Snacks' and pcb.item_category = 'Crisp & Snacks'
            then 'Chip + Chip'
        when (pca.item_category = 'Crisp & Snacks' and pcb.item_category = 'Cheese Crackers, Breadsticks & Dipping')
          or (pca.item_category = 'Cheese Crackers, Breadsticks & Dipping' and pcb.item_category = 'Crisp & Snacks')
            then 'Chip + Dip'
        when pca.item_category in ('Chocolate', 'Other Confectionary')
          and pcb.item_category in ('Chocolate', 'Other Confectionary')
            then 'Choc + Choc'
        else 'Cross-category'
    end as pattern,
    case
        when s.lift >= {{ var('cross_sell_strong_lift_threshold', 1.3) }}
             and s.support >= {{ var('cross_sell_high_support_threshold', 0.02) }}
            then 'Strong Affinity (Bundle Candidate)'
        when s.lift >= {{ var('cross_sell_strong_lift_threshold', 1.3) }}
             and s.support < {{ var('cross_sell_high_support_threshold', 0.02) }}
            then 'Niche Affinity'
        when s.support >= {{ var('cross_sell_high_support_threshold', 0.02) }}
             and s.lift >= {{ var('cross_sell_neutral_lift_lower', 0.9) }}
             and s.lift <= {{ var('cross_sell_neutral_lift_upper', 1.1) }}
            then 'Popularity Effect (Lift ~ 1)'
        when s.lift < {{ var('cross_sell_neutral_lift_lower', 0.9) }}
            then 'Substitution / Avoidance'
        else 'Monitor'
    end as actionability_bucket,
    case
        when s.lift >= {{ var('cross_sell_strong_lift_threshold', 1.3) }}
             and s.support >= {{ var('cross_sell_high_support_threshold', 0.02) }}
            then 1
        when s.lift >= {{ var('cross_sell_strong_lift_threshold', 1.3) }}
             and s.support < {{ var('cross_sell_high_support_threshold', 0.02) }}
            then 2
        when s.support >= {{ var('cross_sell_high_support_threshold', 0.02) }}
             and s.lift >= {{ var('cross_sell_neutral_lift_lower', 0.9) }}
             and s.lift <= {{ var('cross_sell_neutral_lift_upper', 1.1) }}
            then 3
        when s.lift < {{ var('cross_sell_neutral_lift_lower', 0.9) }}
            then 5
        else 4
    end as actionability_rank
from scored as s
left join product_category_map as pca
    on s.product_a = pca.product_name
left join product_category_map as pcb
    on s.product_b = pcb.product_name
order by actionability_rank asc, s.lift desc, s.pair_orders desc
