
-- calculate total sales per transaction_date from latest date to earliest date

{{
    config(
        materialized='incremental',
        unique_key='transaction_date',
        on_schema_change='fail'
    )
}}

select 
    sum(total_spent) as total_sales, transaction_date
from {{ref('silver_cleaned_2')}}
    where (transaction_date is not null) and
    (payment_method is not null) and
    (price_per_unit is not null)


{%if is_incremental() %}
    and transaction_date > (select max(transaction_date) from {{this}})
{% endif %}

group by transaction_date
order by transaction_date desc
