<<<<<<< HEAD
with item_base as (
=======
with item_views_clean as (
>>>>>>> ec2fb7794bcaa16a3f79ab54e96575eeb1150b2c

    select *
    from {{ ref('int_item_views_clean') }}

),

<<<<<<< HEAD
dedup as (
=======
dedup_items as (
>>>>>>> ec2fb7794bcaa16a3f79ab54e96575eeb1150b2c

    select
        item_name,
        price_per_unit,
        row_number() over (
            partition by item_name
<<<<<<< HEAD
            order by item_view_at
        ) as row_n
    from item_base
=======
            order by item_view_at desc
        ) as rn
    from item_views_clean
>>>>>>> ec2fb7794bcaa16a3f79ab54e96575eeb1150b2c
    where item_name is not null

)

select
    item_name,
    price_per_unit
<<<<<<< HEAD
from dedup
where row_n = 1
=======
from dedup_items
where rn = 1
>>>>>>> ec2fb7794bcaa16a3f79ab54e96575eeb1150b2c
