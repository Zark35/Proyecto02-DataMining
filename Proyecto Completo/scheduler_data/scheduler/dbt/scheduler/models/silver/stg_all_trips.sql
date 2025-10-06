{{ config(materialized='view') }}

select * from {{ ref('stg_trips') }}
union all
select * from {{ ref('stg_green_trips') }}
