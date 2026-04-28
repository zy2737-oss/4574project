select *
from {{ source('web_schema', 'item_views') }}