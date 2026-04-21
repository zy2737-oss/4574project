with sessions as (

    select *
    from {{ ref('stg_sessions') }}

)

select
    client_id,
    ip,
    os,
    session_id,
    session_at,
    try_to_timestamp(session_at) as session_at_ts,
    cast(try_to_timestamp(session_at) as date) as session_date
from sessions