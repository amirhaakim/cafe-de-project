-- models/silver_curated/silver_curated.sql

with base as (
    select
        transaction_id,
        item,
        quantity,
        price_per_unit,
        total_spent,
        payment_method,
        location,
        transaction_date
    from {{ ref('silver_cleaned_2') }}
),
corr as (
    select
        transaction_id,
        item,
        quantity,
        price_per_unit,
        total_spent,
        payment_method,
        location,
        transaction_date
    from {{ source('ops', 'silver_corrections') }}
),
final as (
    select
        b.transaction_id,
        coalesce(c.item, b.item) as item,
        coalesce(c.quantity, b.quantity) as quantity,
        coalesce(c.price_per_unit, b.price_per_unit) as price_per_unit,
        coalesce(c.total_spent, b.total_spent, coalesce(c.quantity, b.quantity) * coalesce(c.price_per_unit, b.price_per_unit)
        ) as total_spent,
        case
            when coalesce(c.total_spent, b.total_spent) is not null then true
            else false
        end as is_total_reconciled,
        coalesce(c.payment_method, b.payment_method) as payment_method,
        coalesce(c.location, b.location) as location,
        coalesce(c.transaction_date, b.transaction_date) as transaction_date
    from base b
    left join corr c
        on b.transaction_id = c.transaction_id
)
select * from final
