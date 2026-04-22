{{ config(materialized='view') }}

with sessions as (

    select
        cast(client_id as varchar) as client_id,
        cast(session_id as varchar) as session_id,
        session_at
    from {{ ref('stg_sessions') }}

),

orders as (

    select
        cast(session_id as varchar) as session_id,
        cast(order_id as varchar) as order_id,
        order_at,
        shipping_cost,
        client_name
    from {{ ref('stg_orders') }}

),

session_agg as (

    select
        client_id,
        count(distinct session_id) as total_sessions,
        min(session_at) as first_session_at,
        max(session_at) as last_session_at
    from sessions
    group by client_id

),

order_agg as (

    select
        s.client_id,
        count(distinct o.order_id) as total_orders,
        min(o.order_at) as first_order_at,
        max(o.order_at) as last_order_at,
        coalesce(sum(o.shipping_cost), 0) as total_shipping_cost
    from orders o
    left join sessions s
        on o.session_id = s.session_id
    group by s.client_id

),

final as (

    select
        cast(sa.client_id as varchar) as client_id,
        sa.total_sessions,
        coalesce(oa.total_orders, 0) as total_orders,
        sa.first_session_at,
        sa.last_session_at,
        oa.first_order_at,
        oa.last_order_at,
        coalesce(oa.total_shipping_cost, 0) as total_shipping_cost
    from session_agg sa
    left join order_agg oa
        on sa.client_id = oa.client_id

)

select *
from final