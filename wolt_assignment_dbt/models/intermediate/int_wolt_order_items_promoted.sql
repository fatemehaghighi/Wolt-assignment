with priced as (
    select *
    from {{ ref('int_wolt_order_items_priced') }}
),
promos as (
    select
        {{ surrogate_key(["item_key", "promo_start_date", "promo_end_date", "promo_type", "discount_pct"]) }} as promo_key,
        item_key,
        promo_start_date,
        promo_end_date,
        promo_type,
        discount_pct
    from {{ ref('stg_wolt_promos') }}
),
matched as (
    select
        p.*,
        pr.promo_key,
        pr.promo_type,
        pr.discount_pct,
        row_number() over (
            partition by p.order_item_sk
            order by pr.discount_pct desc, pr.promo_start_date desc
        ) as promo_rank
    from priced as p
    left join promos as pr
        on p.item_key = pr.item_key
        and p.order_date_berlin >= pr.promo_start_date
        and p.order_date_berlin < pr.promo_end_date
)
select
    order_item_sk,
    order_sk,
    customer_sk,
    purchase_key,
    customer_key,
    time_order_received_utc,
    order_date_utc,
    order_date_berlin,
    item_key,
    item_count,
    item_key_sk,
    item_scd_sk,
    item_name_en,
    item_name_de,
    item_name_preferred,
    item_category,
    brand_name,
    unit_base_price_gross_eur,
    vat_rate_pct,
    valid_from_utc,
    valid_to_utc,
    promo_key,
    promo_type,
    coalesce(discount_pct, 0) as discount_pct_applied,
    coalesce(discount_pct, 0) > 0 as is_promo_item,
    round(coalesce(unit_base_price_gross_eur, 0) * cast(item_count as numeric), 2) as line_base_amount_gross_eur,
    round(coalesce(unit_base_price_gross_eur, 0) * (coalesce(discount_pct, 0) / 100.0), 2) as unit_discount_amount_gross_eur,
    round(
        coalesce(unit_base_price_gross_eur, 0)
        - round(coalesce(unit_base_price_gross_eur, 0) * (coalesce(discount_pct, 0) / 100.0), 2),
        2
    ) as unit_final_price_gross_eur,
    round(
        round(coalesce(unit_base_price_gross_eur, 0) * (coalesce(discount_pct, 0) / 100.0), 2)
        * cast(item_count as numeric),
        2
    ) as line_discount_amount_gross_eur,
    round(
        round(
            coalesce(unit_base_price_gross_eur, 0)
            - round(coalesce(unit_base_price_gross_eur, 0) * (coalesce(discount_pct, 0) / 100.0), 2),
            2
        ) * cast(item_count as numeric),
        2
    ) as line_final_amount_gross_eur
from matched
where promo_rank = 1
