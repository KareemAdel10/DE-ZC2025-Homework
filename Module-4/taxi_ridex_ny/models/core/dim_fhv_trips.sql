

with tripdata as (
    select * from {{ ref('stg_staging__fhv_tripdata_2020_03') }}
), 

dim_zones as (
    select * from {{ ref('stg_staging__taxi_zone_lookup') }}
    where locationid not in(264, 265)  
)

select 
    top 100
    tripdata.dispatching_base_num, 
    tripdata.pickup_datetime, 
    tripdata.dropOff_datetime,
    YEAR(tripdata.pickup_datetime) AS year,
    MONTH( tripdata.pickup_datetime) AS month,
    tripdata.PUlocationID, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    tripdata.DOlocationID, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    tripdata.SR_Flag,
    tripdata.Affiliated_base_number

from tripdata
inner join dim_zones as pickup_zone
on tripdata.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on tripdata.DOlocationID = dropoff_zone.locationid
