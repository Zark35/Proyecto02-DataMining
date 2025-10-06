{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('nyc_taxi_raw', 'green_trips_bronze') }}
),

renamed as (
    select
        row_number() over (order by LPEP_PICKUP_DATETIME) as trip_id,

        to_timestamp_ntz(LPEP_PICKUP_DATETIME / 1e9) as pickup_datetime,
        to_timestamp_ntz(LPEP_DROPOFF_DATETIME / 1e9) as dropoff_datetime,

        to_date(to_timestamp_ntz(LPEP_PICKUP_DATETIME / 1e9)) as pickup_date,
        to_date(to_timestamp_ntz(LPEP_DROPOFF_DATETIME / 1e9)) as dropoff_date,

        PASSENGER_COUNT::int as passenger_count,
        TRIP_DISTANCE::float as trip_distance,
        RATECODEID::int as ratecode_id,
        STORE_AND_FWD_FLAG,
        PULOCATIONID::int as pickup_location_id,
        DOLOCATIONID::int as dropoff_location_id,

        PAYMENT_TYPE::int as payment_type,
        FARE_AMOUNT::float as fare_amount,
        EXTRA::float as extra,
        MTA_TAX::float as mta_tax,
        TIP_AMOUNT::float as tip_amount,
        TOLLS_AMOUNT::float as tolls_amount,
        IMPROVEMENT_SURCHARGE::float as improvement_surcharge,
        TOTAL_AMOUNT::float as total_amount,
        CONGESTION_SURCHARGE::float as congestion_surcharge,

        -- GREEN no tiene AIRPORT_FEE
        NULL as airport_fee,

        LOAD_YEAR::int as load_year,
        LOAD_MONTH::int as load_month,
        LOAD_SERVICE_TYPE,

        'green' as service_type
    from source
    where LPEP_PICKUP_DATETIME is not null
      and LPEP_DROPOFF_DATETIME is not null
)

select *
from renamed
