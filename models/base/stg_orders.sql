select
    'FIVETRAN_ID' as fivetran_id,
    "TAX_RATE" as tax_rate,
    "PHONE" as phone,
    "STATE" as state,
    "SESSION_ID" as session_id,
    "ORDER_AT" as order_at,
    "CLIENT_NAME" as client_name,
    "ORDER_ID" as order_id,
    "SHIPPING_ADDRESS" as shipping_address,
    "PAYMENT_METHOD" as payment_method,
    cast(regexp_replace("SHIPPING_COST", '[^0-9.]', '') as float) as shipping_cost,
    "PAYMENT_INFO" as payment_info,
    "_fivetran_deleted" as is_deleted,
    "_fivetran_synced" as fivetran_synced_at
from {{ source('web_schema', 'orders') }}
where "_fivetran_deleted" = false