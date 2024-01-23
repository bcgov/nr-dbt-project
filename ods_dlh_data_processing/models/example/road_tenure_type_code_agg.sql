
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    --select a.*,26 as Total_Count from fdw_ods_rrs_replication.road_tenure_type_code a
    select a.*,count(*) over (partition by expiry_date) as Total_Count from {{ var('db_fdw_rrs') }}.road_tenure_type_code a

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
