with employee_lifecycle as (

    select *
    from {{ ref('int_hr_employee_lifecycle') }}

)

select
    employee_id,
    employee_name,
    city,
    title,
    annual_salary,
    hire_date,
    quit_date,
    has_quit,
    employee_status,
    tenure_days,

    quit_date is null as is_current_employee,

    case
        when annual_salary < 50000 then 'under_50k'
        when annual_salary < 100000 then '50k_to_100k'
        when annual_salary < 150000 then '100k_to_150k'
        when annual_salary >= 150000 then '150k_plus'
        else 'unknown'
    end as salary_band,

    case
        when tenure_days < 30 then 'under_30_days'
        when tenure_days < 90 then '30_to_89_days'
        when tenure_days < 180 then '90_to_179_days'
        when tenure_days >= 180 then '180_plus_days'
        else 'unknown'
    end as tenure_band
from employee_lifecycle
