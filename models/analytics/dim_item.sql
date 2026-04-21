with item_views_clean as (

    select *
    from {{ ref('int_item_views_clean') }}

),

dedup_items as (

    select
        item_name,
        price_per_unit,
        row_number() over (
            partition by item_name
            order by item_view_at
        ) as row_n
    from item_views_clean
    where item_name is not null

)

select
    item_name,
    price_per_unit
from dedup_items
where row_n = 1