-- Task 2 - Q3
-- Do some products get bought together with more frequency than others?

select
    period_month,
    item_key_sk_1,
    item_name_preferred_1,
    item_category_1,
    item_key_sk_2,
    item_name_preferred_2,
    item_category_2,
    orders_together,
    support,
    confidence_1_to_2,
    confidence_2_to_1,
    lift
from `wolt-assignment-489610.analytics_dev_rpt.rpt_item_pair_affinity`
where snapshot_date = (
    select max(snapshot_date)
    from `wolt-assignment-489610.analytics_dev_rpt.rpt_item_pair_affinity`
)
order by period_month, lift desc, orders_together desc;
