-- models/silver/stg_zones.sql
-- Limpieza y estandarización de las zonas de taxi

with source as (
    select *
    from {{ source('nyc_taxi_raw', 'taxi_zones_bronze') }}
),

renamed as (
    select
        LOCATIONID::int as location_id,
        BOROUGH as borough,
        ZONE as zone,
        SERVICE_ZONE as service_zone
    from source
),

cleaned as (
    select distinct
        location_id,
        initcap(borough) as borough,
        initcap(zone) as zone,
        initcap(service_zone) as service_zone
    from renamed
    where location_id is not null
)

select *
from cleaned
