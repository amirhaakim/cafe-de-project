
-- calculate total sales per transaction_date from latest date to earliest date
select 
    sum(total_spent) as total_sales, transaction_date
from {{ref('silver_cleaned_2')}}
    where (transaction_date is not null) and
    (payment_method is not null) and
    (price_per_unit is not null)
    group by transaction_date
    order by transaction_date desc