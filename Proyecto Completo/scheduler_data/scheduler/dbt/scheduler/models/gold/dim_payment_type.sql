{{ config(materialized='table') }}

select distinct
    payment_type as payment_type_sk,
    case
        when payment_type = 1 then 'Credit card'
        when payment_type = 2 then 'Cash'
        when payment_type = 3 then 'No charge'
        when payment_type = 4 then 'Dispute'
        when payment_type = 5 then 'Unknown'
        when payment_type = 6 then 'Voided trip'
        else 'Other'
    end as payment_type_desc
from {{ ref('stg_trips') }}
where payment_type is not null
