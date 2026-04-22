with orders as (

    select *
    from {{ ref('int_orders_clean') }}

),

final as (

    select
        -- primary key
        order_id,

        -- dimensions
        session_id,
        client_name,
        state,
        payment_method,

        -- dates
        order_at,
        order_date,

        -- metrics
        coalesce(shipping_cost, 0) as shipping_cost,
        coalesce(tax_rate, 0) as tax_rate,

        -- derived metrics（如果后面有 revenue 可以扩展）
        coalesce(shipping_cost, 0) * coalesce(tax_rate, 0) as shipping_tax_amount,

        -- metadata
        is_deleted,
        fivetran_synced_at

    from orders
    where order_id is not null

)

select *
from final