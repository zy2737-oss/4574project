WITH hr_joins AS (
    SELECT *
    FROM {{ ref('stg_hr_joins') }}
),

hr_quits AS (
    SELECT *
    FROM {{ ref('stg_hr_quits') }}
),

final AS (
    SELECT
        j.employee_id,
        j.name,
        j.title,
        j.city,
        j.address,
        j.annual_salary,
        j.hire_date,
        q.quit_date,
        CASE
            WHEN q.quit_date IS NULL THEN 'active'
            ELSE 'quit'
        END AS employee_status,
        CASE
            WHEN q.quit_date IS NULL THEN FALSE
            ELSE TRUE
        END AS is_quit
    FROM hr_joins j
    LEFT JOIN hr_quits q ON j.employee_id = q.employee_id
)

SELECT * FROM final