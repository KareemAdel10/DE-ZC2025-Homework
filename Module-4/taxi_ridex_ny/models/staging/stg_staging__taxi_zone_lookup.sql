with 

source as (

    select * from {{ source('staging', 'taxi_zone_lookup') }}

),

renamed as (

    select
        locationid,
        borough,
        zone,
        service_zone

    from source

)

select * from renamed
