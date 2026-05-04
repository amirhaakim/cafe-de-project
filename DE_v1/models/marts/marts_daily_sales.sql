select
    d.full_date as sales_date,
    d.is_unknown as is_unknown_date,
    sum(coalesce(f.total_spent, 0)) as revenue,
    count(distinct f.transaction_id) as transactions,
    sum(coalesce(f.quantity, 0)) as units_sold,
    sum(coalesce(f.total_spent, 0))::numeric
        / nullif(count(distinct f.transaction_id), 0) as avg_ticket,
    sum(coalesce(f.total_spent, 0))::numeric
        / nullif(sum(coalesce(f.quantity, 0)), 0) as avg_selling_price
from {{ ref('fct_table') }} f
join {{ ref('dim_date') }} d
    on f.date_key = d.date_key
group by d.full_date, d.is_unknown
order by d.full_date nulls last
