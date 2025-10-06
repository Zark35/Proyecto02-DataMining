{{ config(materialized='table') }}

with cleaned as (
    select
        try_to_date(to_varchar(pickup_datetime)) as pickup_date,
        try_to_date(to_varchar(dropoff_datetime)) as dropoff_date
    from {{ ref('stg_trips') }}
),

unioned as (
    select pickup_date as date_value from cleaned where pickup_date is not null
    union
    select dropoff_date as date_value from cleaned where dropoff_date is not null
),

filtered as (
    select *
    from unioned
    where date_value between '2015-01-01' and '2025-12-31'
)

select
    row_number() over (order by date_value) as date_sk,
    date_value,
    extract(year from date_value) as year,
    extract(month from date_value) as month,
    extract(day from date_value) as day,
    to_varchar(date_value, 'Mon') as month_name,
    to_varchar(date_value, 'Day') as day_name
from filtered
order by date_value
