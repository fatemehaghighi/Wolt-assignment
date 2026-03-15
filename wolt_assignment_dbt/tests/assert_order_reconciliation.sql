with derived as (
    select
        order_sk,
        sum(order_item_row_final_amount_gross_eur) as derived_basket_value_eur
    from {{ ref('fct_order_item') }}
    group by order_sk
),
orders as (
    select
        order_sk,
        total_basket_value_eur
    from {{ ref('fct_order') }}
)
select
    o.order_sk,
    o.total_basket_value_eur,
    m.derived_basket_value_eur,
    abs(o.total_basket_value_eur - m.derived_basket_value_eur) as abs_diff
from orders as o
left join derived as m
    on o.order_sk = m.order_sk
where abs(o.total_basket_value_eur - coalesce(m.derived_basket_value_eur, 0)) > 0.001
