with orders as (
    select
        {{ surrogate_key(["purchase_key"]) }} as order_sk,
        {{ surrogate_key(["customer_key"]) }} as customer_sk,
        purchase_key,
        customer_key,
        time_order_received_utc,
        order_date_utc,
        order_date_berlin,
        delivery_distance_line_meters,
        total_basket_value_eur,
        wolt_service_fee_eur,
        courier_base_fee_eur,
        total_basket_value_eur + wolt_service_fee_eur as basket_plus_service_fee_eur,
        total_basket_value_eur + wolt_service_fee_eur + courier_base_fee_eur as total_customer_paid_eur
    from {{ ref('stg_wolt_purchase_logs') }}
),
order_item_metrics as (
    select
        order_sk,
        count(*) as distinct_item_count,
        sum(item_count) as total_item_count,
        sum(cast(is_promo_item as int64)) as promo_line_count,
        sum(case when is_promo_item then item_count else 0 end) as promo_item_count,
        sum(line_base_amount_gross_eur) as model_line_base_amount_gross_eur,
        sum(line_discount_amount_gross_eur) as model_line_discount_amount_gross_eur,
        sum(line_final_amount_gross_eur) as model_line_final_amount_gross_eur
    from {{ ref('int_wolt_order_items_promoted') }}
    group by order_sk
),
customer_sequence as (
    select
        *,
        row_number() over (
            partition by customer_sk
            order by time_order_received_utc, purchase_key
        ) as customer_order_number
    from orders
)
select
    c.order_sk,
    c.customer_sk,
    c.purchase_key,
    c.customer_key,
    c.time_order_received_utc,
    c.order_date_utc,
    c.order_date_berlin,
    c.delivery_distance_line_meters,
    c.total_basket_value_eur,
    c.wolt_service_fee_eur,
    c.courier_base_fee_eur,
    c.basket_plus_service_fee_eur,
    c.total_customer_paid_eur,
    coalesce(m.total_item_count, 0) as total_item_count,
    coalesce(m.distinct_item_count, 0) as distinct_item_count,
    coalesce(m.promo_line_count, 0) as promo_line_count,
    coalesce(m.promo_item_count, 0) as promo_item_count,
    coalesce(m.model_line_base_amount_gross_eur, 0) as model_line_base_amount_gross_eur,
    coalesce(m.model_line_discount_amount_gross_eur, 0) as model_line_discount_amount_gross_eur,
    coalesce(m.model_line_final_amount_gross_eur, 0) as model_line_final_amount_gross_eur,
    c.customer_order_number,
    c.customer_order_number = 1 as is_first_order_for_customer,
    coalesce(m.promo_item_count, 0) > 0 as contains_promo_flag
from customer_sequence as c
left join order_item_metrics as m
    on c.order_sk = m.order_sk
