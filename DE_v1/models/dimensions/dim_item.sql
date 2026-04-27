-- models/dimensions/dim_item.sql

with distinct_items as (
    select distinct
        trim(item) as item_name
    from {{ ref('silver_curated') }}
    where item is not null
    and trim(item) <> ''
),
numbered_items as (
    select
        row_number() over (order by item_name) as item_key,
        item_name,
        false as is_unknown
    from distinct_items
)
select
    -1 as item_key,
    'Unknown Item'::text as item_name,
    true as is_unknown
union all
select
    item_key,
    item_name,
    is_unknown
from numbered_items
