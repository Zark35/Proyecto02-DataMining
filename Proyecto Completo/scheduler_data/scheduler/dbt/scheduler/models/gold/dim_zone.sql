{{ config(materialized='table') }}

select
    location_id as zone_sk,
    zone as zone_name,
    borough,
    service_zone
from {{ ref('stg_zones') }}
