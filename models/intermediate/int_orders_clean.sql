with orders as (

    select *
    from {{ ref('stg_orders') }}

),

returns as (

    select *
    from {{ ref('stg_returns') }}

)

select
    o.fivetran_id,
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
    cast(o.order_at as date)        as order_date,
    o.fivetran_synced_at,

    coalesce(r.is_refunded, false)  as is_refunded,
    r.returned_at

from orders o
left join returns r
    on o.order_id = r.order_id