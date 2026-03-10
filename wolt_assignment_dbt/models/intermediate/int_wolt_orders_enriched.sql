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
        -- Task 1 fee-vs-basket comparison helper metric.
        total_basket_value_eur + wolt_service_fee_eur as basket_plus_service_fee_eur,
        -- Task 1 total customer paid view and fee decomposition baseline.
        total_basket_value_eur + wolt_service_fee_eur + courier_base_fee_eur as total_customer_paid_eur
    from {{ ref('int_wolt_purchase_logs_curated') }}
),
order_item_metrics as (
    select
        order_sk,
        count(*) as distinct_order_item_rows_in_order,
        sum(units_in_order_item_row) as total_units_in_order,
        -- Count of promo order-item rows.
        -- Example: [A promo x3, B non-promo x2, C promo x1] => promo_order_item_rows_in_order = 2.
        sum(cast(is_promo_item as int64)) as promo_order_item_rows_in_order,
        -- Count of promo units.
        -- Same example => promo_units_in_order = 3 + 1 = 4.
        sum(case when is_promo_item then units_in_order_item_row else 0 end) as promo_units_in_order,
        sum(order_item_row_base_amount_gross_eur) as modeled_order_items_base_amount_gross_eur,
        sum(order_item_row_discount_amount_gross_eur) as modeled_order_items_discount_amount_gross_eur,
        sum(order_item_row_final_amount_gross_eur) as modeled_order_items_final_amount_gross_eur
    from {{ ref('int_wolt_order_items_promoted') }}
    group by order_sk
),
customer_sequence as (
    select
        *,
        -- Customer order chronology for retention metrics.
        -- row_number() by (customer_sk, event time, purchase_key tie-breaker):
        -- first order = 1 (is_first_order_for_customer = true), later orders are repeats.
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
    coalesce(m.total_units_in_order, 0) as total_units_in_order,
    coalesce(m.distinct_order_item_rows_in_order, 0) as distinct_order_item_rows_in_order,
    coalesce(m.promo_order_item_rows_in_order, 0) as promo_order_item_rows_in_order,
    coalesce(m.promo_units_in_order, 0) as promo_units_in_order,
    coalesce(m.modeled_order_items_base_amount_gross_eur, 0) as modeled_order_items_base_amount_gross_eur,
    coalesce(m.modeled_order_items_discount_amount_gross_eur, 0) as modeled_order_items_discount_amount_gross_eur,
    coalesce(m.modeled_order_items_final_amount_gross_eur, 0) as modeled_order_items_final_amount_gross_eur,
    c.customer_order_number,
    c.customer_order_number = 1 as is_first_order_for_customer,
    coalesce(m.promo_units_in_order, 0) > 0 as has_any_promo_units_in_order
from customer_sequence as c
left join order_item_metrics as m
    on c.order_sk = m.order_sk
