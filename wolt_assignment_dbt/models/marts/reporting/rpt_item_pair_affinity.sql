{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['run_id', 'period_month', 'item_key_sk_1', 'item_key_sk_2'],
        on_schema_change='sync_all_columns',
        partition_by={'field': 'as_of_run_date', 'data_type': 'date'},
        cluster_by=['period_month', 'item_key_sk_1', 'item_key_sk_2']
    )
}}

with pairs as (
    select
        date_trunc(a.order_date, month) as period_month,
        least(a.item_key_sk, b.item_key_sk) as item_key_sk_1,
        greatest(a.item_key_sk, b.item_key_sk) as item_key_sk_2,
        count(distinct a.order_sk) as orders_together
    from {{ ref('fct_order_item') }} as a
    inner join {{ ref('fct_order_item') }} as b
        on a.order_sk = b.order_sk
        and a.item_key_sk < b.item_key_sk
    group by 1, 2, 3
),
item_orders as (
    select
        date_trunc(order_date, month) as period_month,
        item_key_sk,
        count(distinct order_sk) as item_orders
    from {{ ref('fct_order_item') }}
    group by 1, 2
),
total_orders as (
    select
        date_trunc(order_date, month) as period_month,
        count(distinct order_sk) as total_orders
    from {{ ref('fct_order') }}
    group by 1
),
item_dim as (
    select
        item_key_sk,
        item_name_preferred,
        item_category
    from {{ ref('dim_item_current') }}
)
select
    {{ run_id_literal() }} as run_id,
    {{ run_ts_literal() }} as as_of_run_ts,
    {{ run_date_expr() }} as as_of_run_date,
    '{{ var('publish_tag', 'scheduled') }}' as publish_tag,
    p.period_month,
    p.item_key_sk_1,
    p.item_key_sk_2,
    coalesce(i1.item_name_preferred, 'Unknown') as item_name_preferred_1,
    coalesce(i2.item_name_preferred, 'Unknown') as item_name_preferred_2,
    coalesce(i1.item_category, 'Unknown') as item_category_1,
    coalesce(i2.item_category, 'Unknown') as item_category_2,
    p.orders_together,
    safe_divide(p.orders_together, t.total_orders) as support,
    safe_divide(p.orders_together, io1.item_orders) as confidence_1_to_2,
    safe_divide(p.orders_together, io2.item_orders) as confidence_2_to_1,
    safe_divide(
        safe_divide(p.orders_together, io1.item_orders),
        safe_divide(io2.item_orders, t.total_orders)
    ) as lift
from pairs as p
inner join item_orders as io1
    on p.period_month = io1.period_month
    and p.item_key_sk_1 = io1.item_key_sk
inner join item_orders as io2
    on p.period_month = io2.period_month
    and p.item_key_sk_2 = io2.item_key_sk
inner join total_orders as t
    on p.period_month = t.period_month
left join item_dim as i1
    on p.item_key_sk_1 = i1.item_key_sk
left join item_dim as i2
    on p.item_key_sk_2 = i2.item_key_sk
where p.orders_together >= {{ var('pair_affinity_min_orders_together', 5) }}
