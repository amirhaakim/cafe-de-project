
-- calculate total sales per item 
select 
    sum(total_spent) as total_sales, item 
from {{ref('silver_cleaned_2')}}
    where (transaction_date is not null) and
    (payment_method is not null) and
    (price_per_unit is not null) and
    (item is not null)
group by item