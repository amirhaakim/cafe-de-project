-- models/dimensions/dim_payment_method.sql

with distinct_payment_methods as (
    select distinct
        trim(payment_method) as payment_method_name
    from {{ ref('silver_curated') }}
    where payment_method is not null
    and trim(payment_method) <> ''
),

numbered_payment_methods as (
    select
        row_number() over (order by payment_method_name) as payment_method_key,
        payment_method_name,
        'No'::text as is_unknown
    from distinct_payment_methods
)

select
    -1 as payment_method_key,
    'Unknown Payment Method'::text as payment_method_name,
    'Yes'::text as is_unknown

union all

select
    payment_method_key,
    payment_method_name,
    is_unknown
from numbered_payment_methods
