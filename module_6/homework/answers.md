DEZoomCamp 2024 
Module 6 Stream processing
Homework

- Access the Container Shell
`docker exec -it redpanda-1 /bin/bash`

**Question 1: Redpanda version**
1. Execute help command
`rpk help`
2. Check redpanda version
`rpk version`

**Answer:** v22.3.5 (rev 28b2443)

**Question 2: Creating a topic**
1. Execute help command
`rpk help`
2. Create a topic named "test-topic"
`rpk topic create test-topic`

**Answer:** 
```
TOPIC       STATUS
test-topic  OK
```

**Question 3: Connecting to the Kafka server**
**Answer:** True

**Question 4: Sending data to the stream**
How much time did it take? 
sending messages took 0.51 seconds
flushing took 0.00 seconds

Where did it spend most of the time?
**Answer:** Sending the messages

**Question 5: Sending the Trip Data**
```
green_topic = "green-trips-2"

t0 = time.time()
for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send(green_topic, value=row_dict)
t1 = time.time()

producer.flush()

print(f"DONE in {t1-t0} seconds")
```
**Answer:** DONE in 84 seconds

**Question 6. Parsing the data**
```
from pyspark.sql import functions as F

green_stream = green_stream \
  .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
  .select("data.*")

query_2 = green_stream.writeStream.foreachBatch(peek).start()
```

**Answer:** 
Row(lpep_pickup_datetime='2019-10-01 00:57:44', lpep_dropoff_datetime='2019-10-01 01:06:59', PULocationID=25, DOLocationID=228, passenger_count=1.0, trip_distance=2.62, tip_amount=0.0)

**Question 7: Most popular destination**

**Answer:** 
|window                                    |DOLocationID|count|
+------------------------------------------+------------+-----+
|{2024-03-15 12:00:00, 2024-03-15 12:05:00}|74          |35478|




