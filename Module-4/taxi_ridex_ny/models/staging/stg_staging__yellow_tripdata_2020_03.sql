with 

source as (

    select * from {{ source('staging', 'yellow_tripdata_2020_03') }}

),

renamed as (

    select
        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge

    from source

)

select top 10 * from renamed
