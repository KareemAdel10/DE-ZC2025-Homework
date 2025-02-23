with 

source as (

    select * from {{ source('staging', 'green_trip_data_2020_03') }}

),

renamed as (

    select

    from source

)

select * from renamed
