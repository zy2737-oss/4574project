SELECT 
    *,
    TRY_TO_NUMBER(
        REPLACE(REPLACE(EXPENSE_AMOUNT, '$', ''), ' ', '')
    ) AS expense_amount_clean
FROM {{ source('google_drive_load', 'expenses') }}