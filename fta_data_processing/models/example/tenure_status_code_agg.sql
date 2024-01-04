{{ config(materialized='table') }}

with source_data as (

    select * from fta_replication.tenure_status_code a

)

select *
from source_data
