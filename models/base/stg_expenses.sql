select *
from {{ source('google_drive_load', 'EXPENSES') }}