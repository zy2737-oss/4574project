select *
from {{ source('google_drive_load', 'HR_JOINS') }}