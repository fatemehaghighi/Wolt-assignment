-- Products whose observed category changes across order-item history.
-- Expected current dataset: one item_key (Tony's Dark Milk Brownie) moves
-- from Other Confectionary to Chocolate.

with per_item as (
    select
        item_key,
        coalesce(item_name_preferred, item_name_en, item_name_de, 'Unknown Item') as product_name,
        coalesce(item_category, 'Unknown') as item_category,
        min(order_date) as first_seen_order_date,
        max(order_date) as last_seen_order_date,
        count(*) as row_count
    from {{ ref('fct_order_item') }}
    group by 1, 2, 3
),
changes as (
    select
        item_key,
        any_value(product_name) as product_name,
        count(distinct item_category) as distinct_category_count,
        string_agg(
            distinct item_category,
            ', '
            order by item_category
        ) as categories_seen,
        min(first_seen_order_date) as first_seen_order_date,
        max(last_seen_order_date) as last_seen_order_date,
        sum(row_count) as total_rows
    from per_item
    group by 1
)
select *
from changes
where distinct_category_count > 1
order by distinct_category_count desc, total_rows desc, item_key
