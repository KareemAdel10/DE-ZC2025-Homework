with source as (

    select * from {{ source('staging', 'green_tripdata_2020_03') }}

)


select   * from source
