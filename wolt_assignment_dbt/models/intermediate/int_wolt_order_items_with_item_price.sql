with basket_items as (
    select
        oi.purchase_key,
        p.customer_key,
        p.time_order_received_utc,
        p.order_date_utc,
        p.order_date_berlin,
        oi.item_key,
        oi.item_count as units_in_order_item_row
    from {{ ref('stg_wolt_order_items') }} as oi
    inner join {{ ref('int_wolt_purchase_logs_curated_filtered') }} as p
        on oi.purchase_key = p.purchase_key
    where oi.item_key is not null
        and p.time_order_received_utc is not null
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
        b.units_in_order_item_row,
        s.item_key_sk,
        s.item_scd_sk,
        s.item_name_en,
        s.item_name_de,
        s.item_name_preferred,
        s.item_category,
        s.brand_name,
        -- Historical unit price looked up at order event-time from item SCD2.
        s.product_base_price_gross_eur as item_unit_base_price_gross_eur,
        s.vat_rate_pct,
        s.valid_from_utc,
        s.valid_to_utc
    from basket_items as b
    left join {{ ref('int_wolt_item_scd2') }} as s
        on b.item_key = s.item_key
        and b.time_order_received_utc >= s.valid_from_utc
        and b.time_order_received_utc < s.valid_to_utc
    -- Defensive guard:
    -- SCD2 should already provide non-overlapping windows (enforced by tests).
    -- This QUALIFY remains as a last-resort protection for unexpected edge cases
    -- (temporary overlap from incident/backfill/state drift) to avoid duplicate
    -- order-item rows in downstream facts.
    qualify row_number() over (
        partition by b.purchase_key, b.item_key
        order by s.valid_from_utc desc nulls last
            -- Deterministic tie-breakers for rare overlapping/tied SCD2 rows:
            -- 1) prefer wider/later-closing window,
            -- 2) prefer row with concrete price,
            -- 3) prefer latest source event timestamp,
            -- 4) final stable tie-break on surrogate key.
            , s.valid_to_utc desc nulls last
            , case when s.product_base_price_gross_eur is not null and s.product_base_price_gross_eur > 0 then 1 else 0 end desc
            , s.log_item_id desc
            , s.item_scd_sk desc
    ) = 1
)
select * from priced
