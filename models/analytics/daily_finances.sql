with orders as (

    select *
    from {{ ref('int_orders_clean') }}

),

item_views as (

    select *
    from {{ ref('int_item_views_clean') }}

),

revenue_by_session as (

    select
        session_id,
        sum(
            (coalesce(add_to_cart_quantity, 0) - coalesce(remove_from_cart_quantity, 0))
            * coalesce(price_per_unit, 0)
        ) as revenue
    from item_views
    group by session_id

),

final as (

    select
        o.order_date,
        count(distinct o.order_id) as total_orders,
        sum(coalesce(r.revenue, 0)) as total_revenue,
        sum(coalesce(o.shipping_cost, 0)) as total_shipping_cost,
        sum(coalesce(r.revenue, 0) * coalesce(o.tax_rate, 0)) as total_tax_cost,
        sum(coalesce(o.shipping_cost, 0))
            + sum(coalesce(r.revenue, 0) * coalesce(o.tax_rate, 0)) as total_cost,
        sum(coalesce(r.revenue, 0))
            - (
                sum(coalesce(o.shipping_cost, 0))
                + sum(coalesce(r.revenue, 0) * coalesce(o.tax_rate, 0))
            ) as profit
    from orders o
    left join revenue_by_session r
        on o.session_id = r.session_id
    where o.order_date is not null
    group by o.order_date

)

select *
from final
order by order_date