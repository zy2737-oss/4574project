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
        to_varchar(s.client_id) as client_id,
        o.client_name,
        o.phone,
        o.shipping_address,
        o.state,
        o.payment_method,
        o.payment_info,
        o.order_id,
        o.order_at,
        o.fivetran_synced_at
    from orders o
    left join sessions s
        on o.session_id = s.session_id
    where s.client_id is not null

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
    order_at as last_order_at
from ranked_clients
where rn = 1