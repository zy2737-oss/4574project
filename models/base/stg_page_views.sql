select *
from {{ source('web_schema', 'page_views') }}