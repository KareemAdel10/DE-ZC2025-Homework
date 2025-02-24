
with trip_duration_calculated as (
    select
        *,
        DATEDIFF(second, pickup_datetime, dropOff_datetime) as trip_duration
    from {{ ref('dim_fhv_trips') }}
)
select 
    *,
    PERCENTILE_CONT( 0.90) WITHIN GROUP(ORDER BY trip_duration) OVER (PARTITION BY year, month, PUlocationID, DOlocationID) AS trip_duration_p90
from trip_duration_calculated
