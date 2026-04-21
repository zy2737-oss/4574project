with item_base as (

    select *
    from {{ ref('int_item_views_clean') }}

),

dedup as (

    select
        item_name,
        price_per_unit,
        row_number() over (
            partition by item_name
            order by item_view_at
        ) as row_n
    from item_base
    where item_name is not null

)

select
    item_name,
    price_per_unit
from dedup
where row_n = 1
