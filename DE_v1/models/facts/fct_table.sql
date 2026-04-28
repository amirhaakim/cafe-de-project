-- models/facts/fact_sales.sql

with base as (
    select
        transaction_id,
        item,
        payment_method,
        location,
        transaction_date,
        quantity,
        price_per_unit,
        total_spent,
        is_total_reconciled
    from {{ ref('silver_curated') }}
)

select
    b.transaction_id,
    coalesce(dd.date_key, -1) as date_key,
    coalesce(di.item_key, -1) as item_key,
    coalesce(dp.payment_method_key, -1) as payment_method_key,
    coalesce(dl.location_key, -1) as location_key,
    b.quantity,
    b.price_per_unit,
    b.total_spent,
    b.is_total_reconciled
from base b
left join {{ ref('dim_date') }} dd
    on b.transaction_date = dd.full_date
left join {{ ref('dim_item') }} di
    on b.item = di.item_name
left join {{ ref('dim_payment_method') }} dp
    on b.payment_method = dp.payment_method_name
left join {{ ref('dim_location') }} dl
    on b.location = dl.location_name