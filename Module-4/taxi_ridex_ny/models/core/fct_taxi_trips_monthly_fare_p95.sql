
WITH valid_trips AS (
    SELECT
        service_type,
        YEAR(pickup_datetime) AS year,
        MONTH(pickup_datetime) AS month,
        fare_amount

    FROM    
        {{ ref('fact_trips') }}
    WHERE 
        fare_amount > 0
        AND trip_distance > 0
        AND payment_type_description IN ('Cash', 'Credit card')
)
SELECT DISTINCT
    service_type,
    year,
    month,
    PERCENTILE_CONT(0.97) WITHIN GROUP (ORDER BY fare_amount) OVER (PARTITION BY service_type, year, month) AS p97,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fare_amount) OVER (PARTITION BY service_type, year, month) AS p95,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY fare_amount) OVER (PARTITION BY service_type, year, month) AS p90
FROM valid_trips
    

