with orders as (
    select
        purchase_key,
        total_basket_value_eur,
        wolt_service_fee_eur,
        courier_base_fee_eur
    from {{ ref('stg_wolt_purchase_logs') }}
),
order_items as (
    select
        order_item_sk,
        units_in_order_item_row,
        item_unit_base_price_gross_eur,
        order_item_row_final_amount_gross_eur
    from {{ ref('fct_order_item') }}
)
select
    'orders' as entity,
    cast(purchase_key as string) as entity_id
from orders
where total_basket_value_eur < 0
   or wolt_service_fee_eur < 0
   or courier_base_fee_eur < 0

union all

select
    'order_items' as entity,
    cast(order_item_sk as string) as entity_id
from order_items
where units_in_order_item_row <= 0
   or item_unit_base_price_gross_eur < 0
   or order_item_row_final_amount_gross_eur < 0
