Original file is located at
    https://colab.research.google.com/drive/1plqdl33K_HkVx0E0nGJrrkEUssStQsW7

# **Workshop "Data Ingestion with dlt": Homework**

## **Question 1: dlt Version**

- Answer: '1.6.1'

## **Question 2: Define & Run the Pipeline (NYC Taxi API)**
- Pipeline
    ```py
    #2.
    import dlt
    from dlt.sources.helpers.rest_client import RESTClient
    from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


    # your code is here
    @dlt.resource(name="rides", write_disposition="replace")
    def ny_taxi():
        client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
            )
        )
        for page in client.paginate("data_engineering_zoomcamp_api"):
            yield page

    pipeline = dlt.pipeline(
        pipeline_name="ny_taxi_pipeline",
        destination="duckdb",
        dataset_name="ny_taxi_data"
    )

    load_info = pipeline.run(ny_taxi)
    print(load_info)

    pipeline.dataset(dataset_type="default").rides.df()
    ```

- Start a connection to your database using native `duckdb` connection and look what tables were generated:"""

    ```py
    import duckdb

    # Connect to the DuckDB database
    conn = duckdb.connect("ny_taxi_pipeline.duckdb")

    # Set search path to the dataset
    conn.sql("SET search_path = 'ny_taxi_data'")

    # Describe the dataset
    conn.sql("DESCRIBE").df()
    ```

How many tables were created?

* 2
* 4 <----
* 6
* 8
- Answer: 4

## **Question 3: Explore the loaded data**

What is the total number of records extracted?

* 2500
* 5000
* 7500
* 10000 <-------
- Answer: 10000

## **Question 4: Trip Duration Analysis**

* Calculate the average trip duration in minutes.

What is the average trip duration?

* 12.3049 <-----
* 22.3049
* 32.3049
* 42.3049
- Answer: 12.3049
