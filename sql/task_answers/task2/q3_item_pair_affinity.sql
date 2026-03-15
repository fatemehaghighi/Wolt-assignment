-- Task 2 - Q3
-- Do some products get bought together with more frequency than others?

select
    product_a,
    category_a,
    product_b,
    category_b,
    pair_orders,
    support,
    confidence_a_to_b,
    confidence_b_to_a,
    lift,
    expected_pair_orders_independent,
    pair_orders_vs_expected_delta,
    actionability_bucket
from `wolt-assignment-489610.analytics_dev_rpt.rpt_cross_sell_product_pairs`
order by actionability_rank asc, lift desc, pair_orders desc;
