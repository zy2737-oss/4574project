with sessions as (

    select *
    from {{ ref('stg_sessions') }}

),

orders as (

    select *
    from {{ ref('stg_orders') }}

),

-- aggregate sessions → client level
session_agg as (

    select
        client_id,
        count(distinct session_id) as total_sessions,
        min(session_at) as first_session_at,
        max(session_at) as last_session_at
    from sessions
    group by client_id

),

-- aggregate orders → client level
order_agg as (

    select
        client_name,
        count(distinct order_id) as total_orders,
        min(order_at) as first_order_at,
        max(order_at) as last_order_at,
        sum(shipping_cost) as total_shipping_cost
    from orders
    group by client_name

),

final as (

    select
        s.client_id,
        o.client_name,
        s.total_sessions,
        o.total_orders,
        s.first_session_at,
        s.last_session_at,
        o.first_order_at,
        o.last_order_at,
        o.total_shipping_cost

    from session_agg s
    left join order_agg o
        on s.client_id = o.client_name   -- ⚠️ adjust if mismatch

)

select * from final