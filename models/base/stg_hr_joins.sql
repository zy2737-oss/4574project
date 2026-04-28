
with source as (

    select *
    from {{ source('google_drive_load', 'hr_joins') }}

),

cleaned as (

    select
        try_to_number(nullif(trim(to_varchar(employee_id)), '')) as employee_id,

        try_to_date(
            regexp_replace(
                nullif(trim(to_varchar(hire_date)), ''),
                '^day\\s+',
                ''
            )
        ) as hire_date,

        nullif(trim(to_varchar(name)), '') as employee_name,
        initcap(nullif(trim(to_varchar(city)), '')) as city,
        nullif(trim(to_varchar(address)), '') as address,
        lower(nullif(trim(to_varchar(title)), '')) as title,
        try_to_number(nullif(trim(to_varchar(annual_salary)), '')) as annual_salary,

        nullif(trim(to_varchar(_file)), '') as source_file_name,
        try_to_number(nullif(trim(to_varchar(_line)), '')) as source_file_line,
        try_to_timestamp_tz(nullif(trim(to_varchar(_modified)), '')) as source_file_modified_at,
        try_to_timestamp_tz(nullif(trim(to_varchar(_fivetran_synced)), '')) as fivetran_synced_at

    from source

)

select *
from cleaned
where employee_id is not null
