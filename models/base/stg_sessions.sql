select *
from {{ source('web_schema', 'sessions') }}