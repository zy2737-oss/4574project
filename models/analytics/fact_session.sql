with sessions as (

    select *
    from {{ ref('int_sessions_clean') }}

),

item_views as (

    select
        session_id,
        count(*) as num_item_views
    from {{ ref('int_item_views_clean') }}
    group by session_id

),

orders as (

    select
        session_id,
        count(*) as num_orders
    from {{ ref('int_orders_clean') }}
    group by session_id

)

select
    s.session_id,
    s.client_id,
    s.session_date,
    s.ip,
    s.os,
    coalesce(iv.num_item_views, 0) as num_item_views,
    coalesce(o.num_orders, 0) as num_orders,
    case
        when coalesce(iv.num_item_views, 0) > 0 then 1
        else 0
    end as has_item_view,
    case
        when coalesce(o.num_orders, 0) > 0 then 1
        else 0
    end as has_order
from sessions s
left join item_views iv
    on s.session_id = iv.session_id
left join orders o
    on s.session_id = o.session_id