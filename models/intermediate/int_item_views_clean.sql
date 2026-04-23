with item_views as (

    select *
    from {{ ref('stg_item_views') }}

)

select
    session_id,
    item_name,
    item_view_at,
    cast(item_view_at as date) as item_view_date,
    price_per_unit,
    add_to_cart_quantity,
    remove_from_cart_quantity
from item_views