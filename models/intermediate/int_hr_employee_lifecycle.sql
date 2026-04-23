with joins as (

    select *
    from {{ ref('stg_hr_joins') }}

),

quits as (

    select
        employee_id,
        min(quit_date) as quit_date
    from {{ ref('stg_hr_quits') }}
    where quit_date is not null
    group by 1

)

select
    joins.employee_id,
    joins.employee_name,
    joins.city,
    joins.address,
    joins.title,
    joins.annual_salary,
    joins.hire_date,
    quits.quit_date,

    quits.quit_date is not null as has_quit,

    case
        when quits.quit_date is not null then 'former'
        else 'active'
    end as employee_status,

    datediff(
        'day',
        joins.hire_date,
        coalesce(quits.quit_date, current_date)
    ) as tenure_days

from joins
left join quits
    on joins.employee_id = quits.employee_id
