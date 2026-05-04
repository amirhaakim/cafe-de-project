-- dim date table

with base_dates as (
    select distinct
        transaction_date::date as full_date
    from {{ ref('silver_curated') }}
    where transaction_date is not null
),
date_rows as (
    select
        cast(to_char(full_date, 'YYYYMMDD') as integer) as date_key,
        full_date as full_date,
        extract(day from full_date)::int as day_of_month,
        extract(month from full_date)::int as month_number,
        to_char(full_date, 'Mon') as month_name_short,
        extract(quarter from full_date)::int as quarter_number,
        extract(year from full_date)::int as year_number,
        trim(to_char(full_date, 'Day')) as day_name,
        'No'::text as is_unknown
    from base_dates
)
select
    -1 as date_key,
    null::date as full_date,
    null::int as day_of_month,
    null::int as month_number,
    'Unknown'::text as month_name_short,
    null::int as quarter_number,
    null::int as year_number,
    'Unknown'::text as day_name,
    'Yes'::text as is_unknown

union all

select * from date_rows
