with orders as (

    select *
    from {{ ref('stg_orders') }}

)

select
    fivetran_id,
    order_id,
    session_id,
    client_name,
    phone,
    state,
    shipping_address,
    payment_method,
    payment_info,
    shipping_cost,
    tax_rate,
    order_at,
    cast(order_at as date) as order_date,
    fivetran_synced_at
from orders