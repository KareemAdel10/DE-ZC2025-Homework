with fact_trips AS(

SELECT top 10
    service_type,
    YEAR(pickup_datetime) AS year,
    DATEPART(quarter, pickup_datetime) AS Quarter,
    SUM(total_amount) AS revenue
FROM 
    {{ ref('fact_trips') }}
WHERE 
    YEAR(pickup_datetime) IN (2020, 2019)
GROUP BY 
    service_type,
    YEAR(pickup_datetime),
    DATEPART(quarter, pickup_datetime)
)

SELECT 
    *, 
    LAG(REVENUE) OVER(PARTITION BY service_type, Quarter ORDER BY year) AS LAG_total_revenue,
    (revenue - LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year)) / 
    NULLIF(LAG(revenue) OVER (PARTITION BY service_type, quarter ORDER BY year), 0) AS yoy_growth
FROM 
    fact_trips