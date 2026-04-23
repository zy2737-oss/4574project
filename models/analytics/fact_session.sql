with sessions_raw as (

    select *
    from {{ ref('int_sessions_clean') }}

),

<<<<<<< HEAD
item_activity as (
=======
sessions as (

    select
        session_id,
        max(client_id) as client_id,
        min(session_date) as session_date,
        max(ip) as ip,
        max(os) as os
    from sessions_raw
    where session_id is not null
    group by session_id

),

item_views as (
>>>>>>> 3e36fd1de233d8a50fa405a37ee842762546b718

    select
        session_id,
        count(*) as num_item_view_events,
        count(distinct item_name) as num_distinct_items_viewed,
        sum(coalesce(add_to_cart_quantity, 0)) as total_add_to_cart_qty,
        sum(coalesce(remove_from_cart_quantity, 0)) as total_remove_from_cart_qty
    from {{ ref('int_item_views_clean') }}
    where session_id is not null
    group by session_id

),

orders as (

    select
        session_id,
        count(distinct order_id) as num_orders
    from {{ ref('int_orders_clean') }}
    where session_id is not null
    group by session_id

)

select
    s.session_id,
    s.client_id,
    s.session_at,
    s.session_date,
    s.ip,
    s.os,

    coalesce(ia.num_item_view_events, 0) as num_item_view_events,
    coalesce(ia.num_distinct_items_viewed, 0) as num_distinct_items_viewed,
    coalesce(ia.total_add_to_cart_qty, 0) as total_add_to_cart_qty,
    coalesce(ia.total_remove_from_cart_qty, 0) as total_remove_from_cart_qty,
    coalesce(o.num_orders, 0) as num_orders,

    case
        when coalesce(ia.num_item_view_events, 0) > 0 then 1
        else 0
    end as has_item_view,

    case
        when coalesce(ia.total_add_to_cart_qty, 0) > 0 then 1
        else 0
    end as has_add_to_cart,

    case
        when coalesce(ia.total_remove_from_cart_qty, 0) > 0 then 1
        else 0
    end as has_remove_from_cart,

    case
        when coalesce(o.num_orders, 0) > 0 then 1
        else 0
    end as has_order

from sessions s
left join item_activity ia
    on s.session_id = ia.session_id
left join orders o
    on s.session_id = o.session_id
