{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['as_of_run_date', 'customer_sk'],
        on_schema_change='sync_all_columns',
        partition_by={'field': 'as_of_run_date', 'data_type': 'date'},
        cluster_by=['customer_sk']
    )
}}

select
    {{ run_id_literal() }} as run_id,
    {{ run_ts_literal() }} as as_of_run_ts,
    {{ run_date_expr() }} as as_of_run_date,
    '{{ var('publish_tag', 'scheduled') }}' as publish_tag,
    customer_sk,
    min(order_ts_utc) as first_order_ts_utc,
    max(cast(is_first_order_for_customer and contains_promo_flag as int64)) = 1 as first_order_had_promo,
    sum(cast(contains_promo_flag as int64)) as promo_orders,
    sum(cast(not contains_promo_flag as int64)) as non_promo_orders,
    sum(cast(contains_promo_flag as int64)) > 0 and sum(cast(not contains_promo_flag as int64)) = 0 as promo_only_customer_flag,
    count(*) as lifetime_orders
from {{ ref('fct_order') }}
group by customer_sk
