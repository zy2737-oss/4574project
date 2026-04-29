with orders as (

    select *
    from {{ ref('int_orders_clean') }}

),

item_views as (

    select *
    from {{ ref('int_item_views_clean') }}

),

employees as (

    select *
    from {{ ref('int_hr_employee_lifecycle') }}

),

expenses as (

    select *
    from {{ ref('int_daily_expenses') }}

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

returns_by_date as (
 
    select
        o.returned_at                                               as return_date,
        sum(
            (coalesce(iv.add_to_cart_quantity, 0) - coalesce(iv.remove_from_cart_quantity, 0))
            * coalesce(iv.price_per_unit, 0)
        )                                                           as total_returns
    from orders o
    inner join item_views iv
        on o.session_id = iv.session_id
    where o.is_refunded = true
      and o.returned_at is not null
    group by o.returned_at
 
),

order_dates as (

    select distinct order_date
    from orders
    where order_date is not null

),

-- Daily salary cost: prorate each active employee's annual salary by day
daily_salary as (

    select
        d.order_date,
        sum(coalesce(e.annual_salary, 0) / 365.0) as total_daily_salary
    from order_dates d
    inner join employees e
        on e.hire_date <= d.order_date
        and (e.quit_date is null or e.quit_date > d.order_date)
    group by d.order_date

),

final as (

    select
        o.order_date,
        count(distinct o.order_id)                                   as total_orders,
        sum(coalesce(r.revenue, 0))                                  as total_revenue,
        max(coalesce(ret.total_returns, 0))                          as total_refund,
        sum(coalesce(r.revenue, 0))
            - max(coalesce(ret.total_returns, 0))                    as net_revenue,
        sum(coalesce(o.shipping_cost, 0))                            as total_shipping_cost,
        sum(coalesce(r.revenue, 0) * coalesce(o.tax_rate, 0))        as total_tax_cost,

        max(coalesce(ds.total_daily_salary, 0))                      as total_salary_cost,
        max(coalesce(ex.hr_expenses, 0))                             as total_hr_expenses,
        max(coalesce(ex.warehouse_expenses, 0))                      as total_warehouse_expenses,
        max(coalesce(ex.tech_tool_expenses, 0))                      as total_tech_tool_expenses,
        max(coalesce(ex.other_expenses, 0))                          as total_other_expenses,
        max(coalesce(ex.total_daily_expenses, 0))                    as total_operating_expenses,

        -- Total cost: shipping + tax + salary + all operating expense categories
        sum(coalesce(o.shipping_cost, 0))
            + sum(coalesce(r.revenue, 0) * coalesce(o.tax_rate, 0))
            + max(coalesce(ds.total_daily_salary, 0))
            + max(coalesce(ex.total_daily_expenses, 0))              as total_cost,

        -- Profit deducts all cost components from revenue
        sum(coalesce(r.revenue, 0))
            - (
                sum(coalesce(o.shipping_cost, 0))
                + sum(coalesce(r.revenue, 0) * coalesce(o.tax_rate, 0))
                + max(coalesce(ds.total_daily_salary, 0))
                + max(coalesce(ex.total_daily_expenses, 0))
            )                                                        as profit

    from orders o
    left join revenue_by_session r
        on o.session_id = r.session_id
    left join returns_by_date ret
        on o.order_date = ret.return_date
    left join daily_salary ds
        on o.order_date = ds.order_date
    left join expenses ex
        on o.order_date = ex.expense_date
    where o.order_date is not null
    group by o.order_date

)

select *
from final
order by order_date