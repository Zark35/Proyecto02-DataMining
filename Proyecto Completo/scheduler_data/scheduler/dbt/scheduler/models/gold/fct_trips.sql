{{ config(materialized='table') }}

-- ================================================================
-- FACT TABLE: fct_trips
-- Fuente: stg_all_trips (viajes yellow + green)
-- ================================================================

with trips as (
    select
        t.trip_id,
        t.pickup_datetime,
        t.dropoff_datetime,
        t.passenger_count,
        t.trip_distance,
        t.fare_amount,
        t.tip_amount,
        t.total_amount,
        t.ratecode_id,
        t.payment_type,
        t.pickup_location_id,
        t.dropoff_location_id,
        t.load_service_type as service_type
    from {{ ref('stg_all_trips') }} t
),

final as (
    select
        t.trip_id,

        -- Fecha surrogate keys
        d1.date_sk as pickup_date_sk,
        d2.date_sk as dropoff_date_sk,

        -- Zona surrogate keys
        z1.zone_sk as pickup_zone_sk,
        z2.zone_sk as dropoff_zone_sk,

        -- Dimensiones
        p.payment_type_sk,
        r.ratecode_sk,
        v.vendor_sk,

        -- Métricas
        t.passenger_count,
        t.trip_distance,
        t.fare_amount,
        t.tip_amount,
        t.total_amount,

        -- Atributos
        t.service_type,
        datediff('minute', t.pickup_datetime, t.dropoff_datetime) as trip_duration_min

    from trips t
    left join {{ ref('dim_date') }} d1
        on cast(t.pickup_datetime as date) = d1.date_value
    left join {{ ref('dim_date') }} d2
        on cast(t.dropoff_datetime as date) = d2.date_value
    left join {{ ref('dim_zone') }} z1
        on t.pickup_location_id = z1.zone_sk
    left join {{ ref('dim_zone') }} z2
        on t.dropoff_location_id = z2.zone_sk
    left join {{ ref('dim_payment_type') }} p
        on t.payment_type = p.payment_type_sk
    left join {{ ref('dim_ratecode') }} r
        on t.ratecode_id = r.ratecode_sk
    left join {{ ref('dim_vendor') }} v
        on 1 = 1  -- dimensión estática
)

select *
from final
