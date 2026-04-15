select *
from {{ source('web_schema', 'PAGE_VIEWS') }}