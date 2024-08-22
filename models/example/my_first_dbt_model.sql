
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select 
    cast(id as integer) as id,
    cast(id as varchar) as id_varchar
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

where id is not null
