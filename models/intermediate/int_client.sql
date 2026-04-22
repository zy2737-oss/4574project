with sessions as (

    select *
    from {{ ref('stg_sessions') }}

),

orders as (

    select *
    from {{ ref('stg_orders') }}

),

-- aggregate sessions -> client level
session_agg as (

    select
        to_varchar(client_id) as client_id,
        count(distinct session_id) as total_sessions,
        min(session_at) as first_session_at,
        max(session_at) as last_session_at
    from sessions
    where client_id is not null
    group by 1

),

-- attach orders to client_id through session_id
orders_with_client as (

    select
        to_varchar(sessions.client_id) as client_id,
        orders.client_name,
        orders.order_id,
        orders.order_at,
        orders.shipping_cost
    from orders
    left join sessions
        on orders.session_id = sessions.session_id
    where sessions.client_id is not null

),

-- aggregate orders -> client level
order_agg as (

    select
        client_id,
        max(client_name) as client_name,
        count(distinct order_id) as total_orders,
        min(order_at) as first_order_at,
        max(order_at) as last_order_at,
        sum(coalesce(shipping_cost, 0)) as total_shipping_cost
    from orders_with_client
    group by 1

),

final as (

    select
        s.client_id,
        o.client_name,
        s.total_sessions,
        coalesce(o.total_orders, 0) as total_orders,
        s.first_session_at,
        s.last_session_at,
        o.first_order_at,
        o.last_order_at,
        coalesce(o.total_shipping_cost, 0) as total_shipping_cost

    from session_agg s
    left join order_agg o
        on s.client_id = o.client_id

)

select *
from final
