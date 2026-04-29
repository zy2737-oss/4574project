
select
    order_id,
    max(case when lower(trim(is_refunded)) = 'yes' then true else false end) as is_refunded,
    max(returned_at)                as returned_at
from {{ source('google_drive_load', 'returns') }}
where order_id is not null
group by order_id