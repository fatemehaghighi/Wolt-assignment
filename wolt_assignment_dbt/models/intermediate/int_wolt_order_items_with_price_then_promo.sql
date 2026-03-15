with priced as (
    select *
    from {{ ref('int_wolt_order_items_with_item_price') }}
),
promos as (
    select
        {{ surrogate_key(["item_key", "promo_start_date", "promo_end_date", "promo_type", "discount_pct"]) }} as promo_sk,
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
        pr.promo_sk,
        pr.promo_type,
        pr.discount_pct,
        -- With clean promo source, each order item should match at most one promo window.
        -- We still rank defensively so unexpected overlaps/duplicates do not duplicate fact rows.
        row_number() over (
            partition by p.order_item_sk
            order by pr.discount_pct desc, pr.promo_start_date desc
        ) as promo_rank
    from priced as p
    left join promos as pr
        on p.item_key = pr.item_key
        -- Promo windows are date-based business rules in assignment:
        -- start date is inclusive from local midnight,
        -- end date is exclusive from local midnight.
        -- Therefore match on Berlin local order DATE (not timestamp).
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
    units_in_order_item_row,
    item_key_sk,
    item_scd_sk,
    item_name_en,
    item_name_de,
    item_name_preferred,
    item_category,
    brand_name,
    item_unit_base_price_gross_eur,
    vat_rate_pct,
    valid_from_utc,
    valid_to_utc,
    promo_sk,
    promo_type,
    coalesce(discount_pct, 0) as discount_pct_applied,
    coalesce(discount_pct, 0) > 0 as is_promo_item,
    -- Task 1/2 monetary decomposition at order-item-row level (base, discount, final).
    coalesce(item_unit_base_price_gross_eur, 0) * cast(units_in_order_item_row as numeric) as order_item_row_base_amount_gross_eur,
    coalesce(item_unit_base_price_gross_eur, 0) * (coalesce(discount_pct, 0) / 100.0) as item_unit_discount_amount_gross_eur,
    coalesce(item_unit_base_price_gross_eur, 0) * (1 - (coalesce(discount_pct, 0) / 100.0)) as item_unit_final_price_gross_eur,
    coalesce(item_unit_base_price_gross_eur, 0) * cast(units_in_order_item_row as numeric) * (coalesce(discount_pct, 0) / 100.0) as order_item_row_discount_amount_gross_eur,
    coalesce(item_unit_base_price_gross_eur, 0) * cast(units_in_order_item_row as numeric) * (1 - (coalesce(discount_pct, 0) / 100.0)) as order_item_row_final_amount_gross_eur
from matched
where promo_rank = 1
