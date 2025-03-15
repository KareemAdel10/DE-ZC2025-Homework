from time import time,sleep
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import pandas as pd


df = pd.read_csv(r'K:\BEST\Boot Camp\Zoom Camp\data-engineering-zoomcamp\06-streaming\pyflink\green_tripdata_2019-10.csv')

df_new = df[['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount']]

df_new = df_new.fillna({
    'passenger_count': 0,
    'trip_distance': 0.0,
    'tip_amount': 0.0
})

def serialize(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

topic = 'green-trips'

producer = KafkaProducer(
    bootstrap_servers = [server],
    value_serializer = serialize
    )

data = df_new.to_dict(orient='records')

t0 = time()

for message in data:
    producer.send(topic, value=message)
    
producer.flush()

t1 = time()
took = t1 - t0

print(f"Sent {len(data)} messages. Time taken: {took} seconds")