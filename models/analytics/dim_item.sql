<<<<<<< HEAD
<<<<<<< HEAD
with item_base as (
=======
with item_views_clean as (
>>>>>>> ec2fb7794bcaa16a3f79ab54e96575eeb1150b2c
=======
with item_views_clean as (
>>>>>>> 065015e82207dc303292a8058969dc7266cbc07f

    select *
    from {{ ref('int_item_views_clean') }}

),

<<<<<<< HEAD
<<<<<<< HEAD
dedup as (
=======
dedup_items as (
>>>>>>> ec2fb7794bcaa16a3f79ab54e96575eeb1150b2c
=======
dedup_items as (
>>>>>>> 065015e82207dc303292a8058969dc7266cbc07f

    select
        item_name,
        price_per_unit,
        row_number() over (
            partition by item_name
<<<<<<< HEAD
<<<<<<< HEAD
            order by item_view_at
        ) as row_n
    from item_base
=======
            order by item_view_at desc
        ) as rn
    from item_views_clean
>>>>>>> ec2fb7794bcaa16a3f79ab54e96575eeb1150b2c
=======
            order by item_view_at
        ) as row_n
    from item_views_clean
>>>>>>> 065015e82207dc303292a8058969dc7266cbc07f
    where item_name is not null

)

select
    item_name,
    price_per_unit
<<<<<<< HEAD
<<<<<<< HEAD
from dedup
where row_n = 1
=======
from dedup_items
where rn = 1
>>>>>>> ec2fb7794bcaa16a3f79ab54e96575eeb1150b2c
=======
from dedup_items
where row_n = 1
>>>>>>> 065015e82207dc303292a8058969dc7266cbc07f
