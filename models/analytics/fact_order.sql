with orders as (

    select *
    from {{ ref('int_orders_clean') }}

),

order_items as (

    select
        session_id,
        item_name,
        price_per_unit,
        add_to_cart_quantity
    from {{ ref('int_item_views_clean') }}
    where add_to_cart_quantity is not null

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
    o.order_date
from orders o
