{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['order_month', 'item_category'],
        on_schema_change='sync_all_columns',
        partition_by={'field': 'order_month', 'data_type': 'date'},
        cluster_by=['order_month', 'item_category'],
        pre_hook=ensure_run_metadata_table(),
        post_hook=upsert_run_metadata()
    )
}}

with base as (
    select
        date_trunc(oi.order_date, month) as order_month,
        coalesce(oi.item_category, 'Unknown') as item_category,
        oi.order_sk,
        oi.customer_sk,
        oi.units_in_order_item_row as units_sold,
        oi.order_item_row_final_amount_gross_eur as revenue_eur,
        oi.order_item_row_base_amount_gross_eur as base_revenue_eur,
        oi.order_item_row_discount_amount_gross_eur as discount_eur,
        case when oi.is_promo_item then oi.units_in_order_item_row else 0 end as promo_units_sold,
        coalesce(oi.item_name_preferred, oi.item_name_en, oi.item_name_de, 'Unknown Item') as product_name
    from {{ ref('fct_order_item') }} as oi
),
monthly_totals as (
    select
        order_month,
        sum(revenue_eur) as total_revenue_eur,
        count(distinct order_sk) as total_orders,
        count(distinct customer_sk) as total_customers
    from base
    group by 1
),
category_monthly as (
    select
        order_month,
        item_category,
        sum(revenue_eur) as revenue_eur,
        sum(base_revenue_eur) as base_revenue_eur,
        sum(discount_eur) as discount_eur,
        sum(units_sold) as units_sold,
        sum(promo_units_sold) as promo_units_sold,
        count(distinct order_sk) as orders,
        count(distinct customer_sk) as customers,
        count(distinct product_name) as sku_count
    from base
    group by 1, 2
),
customer_category_monthly as (
    select
        order_month,
        item_category,
        customer_sk,
        count(distinct order_sk) as customer_orders_in_category_month
    from base
    group by 1, 2, 3
),
repeat_customer_monthly as (
    select
        order_month,
        item_category,
        safe_divide(
            count(distinct case when customer_orders_in_category_month > 1 then customer_sk end),
            nullif(count(distinct customer_sk), 0)
        ) as repeat_customer_rate
    from customer_category_monthly
    group by 1, 2
),
product_monthly as (
    select
        order_month,
        item_category,
        product_name,
        sum(revenue_eur) as product_revenue_eur
    from base
    group by 1, 2, 3
),
product_ranked as (
    select
        *,
        row_number() over (
            partition by order_month, item_category
            order by product_revenue_eur desc, product_name
        ) as product_rank
    from product_monthly
),
concentration as (
    select
        order_month,
        item_category,
        safe_divide(
            sum(case when product_rank = 1 then product_revenue_eur else 0 end),
            nullif(sum(product_revenue_eur), 0)
        ) as top_sku_share,
        safe_divide(
            sum(case when product_rank <= 3 then product_revenue_eur else 0 end),
            nullif(sum(product_revenue_eur), 0)
        ) as top_3_share
    from product_ranked
    group by 1, 2
),
order_categories as (
    select distinct
        order_month,
        order_sk,
        item_category
    from base
),
cross_sell_pairs as (
    select
        a.order_month,
        a.item_category as base_category,
        b.item_category as attached_category,
        count(distinct a.order_sk) as orders_with_both_categories
    from order_categories as a
    join order_categories as b
        on a.order_month = b.order_month
        and a.order_sk = b.order_sk
        and a.item_category <> b.item_category
    group by 1, 2, 3
),
cross_sell_ranked as (
    select
        p.order_month,
        p.base_category as item_category,
        p.attached_category as top_attached_category,
        safe_divide(p.orders_with_both_categories, nullif(c.orders, 0)) as cross_sell_attach_rate,
        row_number() over (
            partition by p.order_month, p.base_category
            order by safe_divide(p.orders_with_both_categories, nullif(c.orders, 0)) desc,
                     p.orders_with_both_categories desc,
                     p.attached_category
        ) as rn
    from cross_sell_pairs as p
    join category_monthly as c
        on p.order_month = c.order_month
        and p.base_category = c.item_category
),
order_monthly as (
    select
        date_trunc(o.order_date, month) as order_month,
        oc.item_category,
        count(distinct case when o.is_first_order_for_customer then o.customer_sk end) as distinct_customers_whose_first_order_included_category_in_month,
        count(distinct case when not o.is_first_order_for_customer then o.customer_sk end) as distinct_customers_with_repeat_orders_including_category_in_month,
        avg(o.delivery_distance_line_meters) as avg_delivery_distance_meters,
        avg(o.total_units_in_order) as avg_order_units_for_orders_with_category
    from {{ ref('fct_order') }} as o
    join (
        select distinct order_sk, item_category
        from order_categories
    ) as oc
        on o.order_sk = oc.order_sk
    group by 1, 2
)
select
    c.order_month,
    c.item_category,

    -- Core requested business metrics
    c.revenue_eur,
    c.base_revenue_eur,
    c.discount_eur,
    safe_divide(c.discount_eur, nullif(c.base_revenue_eur, 0)) as discount_rate,
    c.units_sold,
    c.orders,
    c.customers,
    safe_divide(c.revenue_eur, nullif(t.total_revenue_eur, 0)) as revenue_share,
    safe_divide(c.orders, nullif(t.total_orders, 0)) as order_penetration,
    safe_divide(c.customers, nullif(t.total_customers, 0)) as customer_penetration,
    safe_divide(c.revenue_eur, nullif(c.units_sold, 0)) as asp_eur,
    safe_divide(c.revenue_eur, nullif(c.orders, 0)) as revenue_per_order_eur,
    safe_divide(c.units_sold, nullif(c.orders, 0)) as units_per_order,
    safe_divide(c.orders, nullif(c.customers, 0)) as orders_per_customer,
    coalesce(r.repeat_customer_rate, 0) as repeat_customer_rate,
    c.sku_count,
    coalesce(k.top_sku_share, 0) as top_sku_share,
    coalesce(k.top_3_share, 0) as top_3_share,
    x.top_attached_category,
    coalesce(x.cross_sell_attach_rate, 0) as cross_sell_attach_rate,

    -- Backward-compatible aliases used by existing dashboards/queries
    c.revenue_eur as order_item_rows_revenue_eur,
    c.discount_eur as order_item_rows_discount_eur,
    safe_divide(c.revenue_eur, nullif(c.units_sold, 0)) as weighted_avg_selling_price_eur,
    c.customers as distinct_customers_in_month,
    coalesce(o.distinct_customers_whose_first_order_included_category_in_month, 0) as distinct_customers_whose_first_order_included_category_in_month,
    coalesce(o.distinct_customers_with_repeat_orders_including_category_in_month, 0) as distinct_customers_with_repeat_orders_including_category_in_month,
    c.promo_units_sold,
    o.avg_order_units_for_orders_with_category,
    o.avg_delivery_distance_meters
from category_monthly as c
join monthly_totals as t
    on c.order_month = t.order_month
left join repeat_customer_monthly as r
    on c.order_month = r.order_month
    and c.item_category = r.item_category
left join concentration as k
    on c.order_month = k.order_month
    and c.item_category = k.item_category
left join (
    select
        order_month,
        item_category,
        top_attached_category,
        cross_sell_attach_rate
    from cross_sell_ranked
    where rn = 1
) as x
    on c.order_month = x.order_month
    and c.item_category = x.item_category
left join order_monthly as o
    on c.order_month = o.order_month
    and c.item_category = o.item_category
