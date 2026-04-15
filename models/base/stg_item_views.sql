select *
from {{ source('web_schema', 'ITEM_VIEWS') }}