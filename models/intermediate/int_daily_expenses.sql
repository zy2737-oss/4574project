with expenses as (
 
    select *
    from {{ ref('stg_expenses') }}
 
)
 
select
    date as expense_date,
 
    sum(case when lower(expense_type) = 'hr'         then coalesce(expense_amount_clean, 0) else 0 end) as hr_expenses,
    sum(case when lower(expense_type) = 'warehouse'  then coalesce(expense_amount_clean, 0) else 0 end) as warehouse_expenses,
    sum(case when lower(expense_type) = 'tech tool'  then coalesce(expense_amount_clean, 0) else 0 end) as tech_tool_expenses,
    sum(case when lower(expense_type) not in ('hr', 'warehouse', 'tech tool')
                                                     then coalesce(expense_amount_clean, 0) else 0 end) as other_expenses,
 
    sum(coalesce(expense_amount_clean, 0))            as total_daily_expenses
 
from expenses
group by 1
order by 1