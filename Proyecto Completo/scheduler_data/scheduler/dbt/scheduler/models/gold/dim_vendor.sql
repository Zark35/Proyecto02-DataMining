{{ config(materialized='table') }}

-- Dimensión estática de proveedores (no depende del staging)
select * from (
    select 1 as vendor_sk, 'Creative Mobile Technologies (CMT)' as vendor_name
    union all
    select 2 as vendor_sk, 'VeriFone Inc.' as vendor_name
    union all
    select 0 as vendor_sk, 'Unknown' as vendor_name
) v
