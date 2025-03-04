# Module 1 Homework: Docker & SQL

## Question 1. Understanding docker first run

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

- 24.3.1 <---
- 24.2.1
- 23.3.1
- 23.2.1

## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

* postgres:5433
* localhost:5432
* db:5433
* postgres:5432 < ---
* db:5432 <---

## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:

1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles

Answers:

- 104,793;  197,670;  110,612;  27,831;  35,281
- 104,793;  198,924;  109,603;  27,678;  35,189
- 101,056;  201,407;  110,612;  27,831;  35,281
- 101,056;  202,661;  109,603;  27,678;  35,189
- 104,838;  199,013;  109,645;  27,688;  35,202 <---

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance.

- 2019-10-11
- 2019-10-24
- 2019-10-26
- 2019-10-31 <---

## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.

- East Harlem North, East Harlem South, Morningside Heights <---
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park

## Question 6. Largest tip

For the passengers picked up in Ocrober 2019 in the zone
name "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

- Yorkville West
- JFK Airport <--
- East Harlem North
- East Harlem South

## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for:

1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:

- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-aprove, terraform destroy
- terraform init, terraform apply -auto-aprove, terraform destroy <---
- terraform import, terraform apply -y, terraform rm

# Sql_Solutions

```sql
SELECT 
	*
FROM 
	green_taxi_data
WHERE
	to_char(lpep_pickup_datetime,'yyyy-mm-dd')= '2019-10-18'
LIMIT 5;

SELECT 
	*
FROM 
	taxi_zone_lookup
LIMIT 5;
-- Question 3. Trip Segmentation Count
---- Up to 1 mile
select 
	count(*) 
FROM
	green_taxi_data 
WHERE
	trip_distance <= 1

----In between 1 (exclusive) and 3 miles (inclusive),
select 
	count(*) 
FROM
	green_taxi_data 
WHERE
	trip_distance > 1 AND trip_distance <= 3

----In between 3 (exclusive) and 7 miles (inclusive),
select 
	count(*) 
FROM
	green_taxi_data 
WHERE
	trip_distance > 3 AND trip_distance <= 7

----In between 7 (exclusive) and 10 miles (inclusive),
select 
	count(*) 
FROM
	green_taxi_data 
WHERE
	trip_distance > 7 AND trip_distance <= 10

---- Over 10 miles
select 
	count(*) 
FROM
	green_taxi_data 
WHERE
	trip_distance > 10

-- Question 4. Longest trip for each day
SELECT 
	EXTRACT(day FROM lpep_pickup_datetime) AS Day,
	MAX(trip_distance) AS Longest_trip
FROM 
	green_taxi_data
GROUP BY 
	EXTRACT(day FROM lpep_pickup_datetime)
ORDER BY Longest_trip desc
LIMIT 1;

-- Question 5. Three biggest pickup zones
WITH cte AS(
SELECT 
	"PULocationID",
	SUM(Total_amount) Total_amount
FROM 
	public.green_taxi_data 
WHERE 
	to_char(lpep_pickup_datetime,'yyyy-mm-dd')= '2019-10-18'
GROUP BY 
	"PULocationID"
HAVING 
	SUM(Total_amount) > 13000
)
SELECT 
	lk."Zone",
	total_amount
FROM 
	cte
JOIN
	taxi_zone_lookup lk ON cte."PULocationID" = lk."LocationID"
ORDER BY 
	total_amount DESC
LIMIT 3; -- It's only 3 zones that adhere to the having clause anyway

-- Question 6. Largest tip
WITH cte AS(
SELECT 
	*
	-- , ROW_NUMBER() OVER(ORDER BY tip_amount desc) we could use a ranking function  also.
FROM 
	green_taxi_data g 
	JOIN taxi_zone_lookup lk ON g."PULocationID" = lk."LocationID"
WHERE 
	lk."Zone" ='East Harlem North' 
),
Highest_tip AS
(
	SELECT 
		*
	FROM 
		cte
	WHERE tip_amount = (SELECT MAX(tip_amount) FROM cte)
)
SELECT 
	lk."Zone"
FROM highest_tip ht JOIN public.taxi_zone_lookup lk ON ht."DOLocationID" = lk."LocationID"
```
