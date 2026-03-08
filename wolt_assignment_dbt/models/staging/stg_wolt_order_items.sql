with exploded as (
    select
        p.purchase_key,
        json_value(basket_item, '$.item_key') as item_key,
        cast(json_value(basket_item, '$.item_count') as int64) as item_count
    from {{ ref('stg_wolt_purchase_logs') }} as p,
        unnest(json_query_array(p.item_basket_description_json, '$')) as basket_item
)
select
    purchase_key,
    item_key,
    sum(item_count) as item_count
from exploded
where item_key is not null
    and item_count is not null
    and item_count > 0
group by 1, 2
