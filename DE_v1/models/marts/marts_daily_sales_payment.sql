with daily_payment as (
    select
        d.full_date as sales_date,
        d.is_unknown as is_unknown_date,
        p.payment_method_key,
        p.payment_method_name,
        p.is_unknown as is_unknown_payment_method,
        sum(coalesce(f.total_spent, 0)) as revenue,
        count(distinct f.transaction_id) as transactions,
        sum(coalesce(f.quantity, 0)) as units_sold
    from {{ ref('fct_table') }} f
    join {{ ref('dim_date') }} d
        on f.date_key = d.date_key
    join {{ ref('dim_payment_method') }} p
        on f.payment_method_key = p.payment_method_key
    group by d.full_date, d.is_unknown, p.payment_method_key, p.payment_method_name, p.is_unknown
)
select
    sales_date,
    is_unknown_date,
    payment_method_key,
    payment_method_name,
    is_unknown_payment_method,
    revenue,
    transactions,
    units_sold,
    revenue::numeric
        / nullif(sum(revenue) over (partition by sales_date, is_unknown_date), 0) as payment_revenue_share_pct,
    revenue::numeric / nullif(transactions, 0) as avg_ticket,
    revenue::numeric / nullif(units_sold, 0) as avg_selling_price
from daily_payment
order by sales_date nulls last, revenue desc, payment_method_name
