with orders as (

    select *
    from {{ ref('stg_orders') }}

),

sessions as (

    select *
    from {{ ref('stg_sessions') }}

),

orders_with_client as (

    select
        to_varchar(sessions.client_id) as client_id,
        orders.client_name,
        orders.phone,
        orders.shipping_address,
        orders.state,
        orders.payment_method,
        orders.payment_info,
        orders.order_id,
        orders.order_at,
        orders.fivetran_synced_at

    from orders
    left join sessions
        on orders.session_id = sessions.session_id

    where sessions.client_id is not null

),

ranked_clients as (

    select
        client_id,
        client_name,
        phone,
        shipping_address,
        state,
        payment_method,
        payment_info,
        order_id,
        order_at,
        row_number() over (
            partition by client_id
            order by order_at desc, fivetran_synced_at desc
        ) as rn

    from orders_with_client

)

select
    client_id,
    client_name,
    phone,
    shipping_address as latest_shipping_address,
    state as latest_state,
    payment_method as latest_payment_method,
    payment_info as latest_payment_info,
    order_id as latest_order_id,
    order_at as latest_order_at

from ranked_clients
where rn = 1
