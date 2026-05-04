with daily_location as (
    select
        d.full_date as sales_date,
        d.is_unknown as is_unknown_date,
        l.location_key,
        l.location_name,
        l.is_unknown as is_unknown_location,
        sum(coalesce(f.total_spent, 0)) as revenue,
        count(distinct f.transaction_id) as transactions,
        sum(coalesce(f.quantity, 0)) as units_sold
    from {{ ref('fct_table') }} f
    join {{ ref('dim_date') }} d
        on f.date_key = d.date_key
    join {{ ref('dim_location') }} l
        on f.location_key = l.location_key
    group by d.full_date, d.is_unknown, l.location_key, l.location_name, l.is_unknown
)
select
    sales_date,
    is_unknown_date,
    location_key,
    location_name,
    is_unknown_location,
    revenue,
    transactions,
    units_sold,
    revenue::numeric
        / nullif(sum(revenue) over (partition by sales_date, is_unknown_date), 0) as location_revenue_share_pct,
    revenue::numeric / nullif(transactions, 0) as avg_ticket,
    revenue::numeric / nullif(units_sold, 0) as avg_selling_price
from daily_location
order by sales_date nulls last, revenue desc, location_name
