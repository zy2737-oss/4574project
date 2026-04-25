with orders as (

    select *
    from {{ ref('int_orders_clean') }}

),

item_summary as (

    select
        session_id,
        sum(coalesce(add_to_cart_quantity, 0)) as total_add_to_cart_qty,
        sum(coalesce(remove_from_cart_quantity, 0)) as total_remove_from_cart_qty,
        count(distinct item_name) as num_distinct_items,
        sum(
            (coalesce(add_to_cart_quantity, 0) - coalesce(remove_from_cart_quantity, 0))
            * coalesce(price_per_unit, 0)
        ) as estimated_revenue
    from {{ ref('int_item_views_clean') }}
    group by session_id

)

select
    o.order_id,
    o.session_id,
    o.client_name,
    o.phone,
    o.state,
    o.shipping_address,
    o.payment_method,
    o.payment_info,
    o.shipping_cost,
    o.tax_rate,
    o.order_at,
    o.order_date,
    coalesce(i.total_add_to_cart_qty, 0) as total_add_to_cart_qty,
    coalesce(i.total_remove_from_cart_qty, 0) as total_remove_from_cart_qty,
    coalesce(i.num_distinct_items, 0) as num_distinct_items,
    coalesce(i.estimated_revenue, 0) as estimated_revenue
from orders o
left join item_summary i
    on o.session_id = i.session_id
    