with employee_lifecycle as (

    select *
    from {{ ref('int_hr_employee_lifecycle') }}

),

ranked_employees as (

    select
        employee_id,
        employee_name,
        city,
        address,
        title,
        annual_salary,
        hire_date,
        quit_date,
        has_quit,
        employee_status,
        tenure_days,
        row_number() over (
            partition by employee_id
            order by hire_date desc, quit_date desc
        ) as rn
    from employee_lifecycle

)

select
    employee_id,
    employee_name,
    city,
    address,
    title,
    annual_salary,
    hire_date,
    quit_date,
    has_quit,
    employee_status,
    tenure_days
from ranked_employees
where rn = 1
