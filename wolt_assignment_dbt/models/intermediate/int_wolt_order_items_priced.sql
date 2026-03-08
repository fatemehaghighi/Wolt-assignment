with basket_items as (
    select *
    from {{ ref('stg_wolt_order_items') }}
),
priced as (
    select
        {{ surrogate_key(["b.purchase_key", "b.item_key"]) }} as order_item_sk,
        {{ surrogate_key(["b.purchase_key"]) }} as order_sk,
        {{ surrogate_key(["b.customer_key"]) }} as customer_sk,
        b.purchase_key,
        b.customer_key,
        b.time_order_received_utc,
        b.order_date_utc,
        b.order_date_berlin,
        b.item_key,
        b.item_count,
        s.item_key_sk,
        s.item_scd_key,
        s.item_name_en,
        s.item_name_de,
        s.item_name_preferred,
        s.item_category,
        s.brand_name,
        s.product_base_price_gross_eur as unit_base_price_gross_eur,
        s.vat_rate_pct,
        s.valid_from_utc,
        s.valid_to_utc
    from basket_items as b
    left join {{ ref('int_wolt_item_scd2') }} as s
        on b.item_key = s.item_key
        and b.time_order_received_utc >= s.valid_from_utc
        and b.time_order_received_utc < s.valid_to_utc
    qualify row_number() over (
        partition by b.purchase_key, b.item_key
        order by s.valid_from_utc desc nulls last
    ) = 1
)
select * from priced
