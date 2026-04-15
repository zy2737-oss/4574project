select *
from {{ source('web_schema', 'SESSIONS') }}