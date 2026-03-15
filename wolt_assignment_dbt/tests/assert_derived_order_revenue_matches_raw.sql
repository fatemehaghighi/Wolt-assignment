-- For each order, derived basket revenue from order-item rows must reconcile with raw purchase total.
-- Fails when raw row is missing, raw total is unparsable, duplicate raw keys exist, or value mismatch exceeds tolerance.

with raw_base as (
    select
        purchase_key,
        safe_cast(total_basket_value as numeric) as raw_total_basket_value_eur
    from {{ source('raw', 'wolt_snack_store_purchase_logs') }}
),
raw_by_order as (
    select
        purchase_key,
        count(*) as raw_row_count,
        count(distinct raw_total_basket_value_eur) as raw_distinct_total_count,
        max(raw_total_basket_value_eur) as raw_total_basket_value_eur
    from raw_base
    group by 1
),
derived_by_order as (
    select
        o.purchase_key,
        coalesce(sum(oi.order_item_row_final_amount_gross_eur), 0) as derived_basket_value_eur
    from {{ ref('fct_order') }} as o
    left join {{ ref('fct_order_item') }} as oi
        on o.order_sk = oi.order_sk
    group by 1
)
select
    d.purchase_key,
    d.derived_basket_value_eur,
    r.raw_total_basket_value_eur,
    r.raw_row_count,
    r.raw_distinct_total_count,
    abs(d.derived_basket_value_eur - coalesce(r.raw_total_basket_value_eur, 0)) as abs_diff,
    case
        when r.purchase_key is null then 'missing_raw_purchase_key'
        when r.raw_row_count != 1 then 'duplicate_raw_purchase_key_rows'
        when r.raw_total_basket_value_eur is null then 'raw_total_basket_value_unparsable'
        when abs(d.derived_basket_value_eur - r.raw_total_basket_value_eur) > 0.001 then 'derived_vs_raw_value_mismatch'
        else 'ok'
    end as failure_reason
from derived_by_order as d
left join raw_by_order as r
    on d.purchase_key = r.purchase_key
where r.purchase_key is null
    or r.raw_row_count != 1
    or r.raw_total_basket_value_eur is null
    or abs(d.derived_basket_value_eur - r.raw_total_basket_value_eur) > 0.001
