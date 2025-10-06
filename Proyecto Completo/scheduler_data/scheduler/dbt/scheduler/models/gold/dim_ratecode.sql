{{ config(materialized='table') }}

select distinct
    ratecode_id as ratecode_sk,
    case
        when ratecode_id = 1 then 'Standard rate'
        when ratecode_id = 2 then 'JFK'
        when ratecode_id = 3 then 'Newark'
        when ratecode_id = 4 then 'Nassau or Westchester'
        when ratecode_id = 5 then 'Negotiated fare'
        when ratecode_id = 6 then 'Group ride'
        else 'Unknown'
    end as ratecode_desc
from {{ ref('stg_trips') }}
where ratecode_id is not null
