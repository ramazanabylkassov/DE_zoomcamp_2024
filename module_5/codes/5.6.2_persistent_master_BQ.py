import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()
    # .master("spark://de-zoomcamp.europe-west1-b.c.fresh-ocean-412204.internal:7077") \

spark.conf.set('temporaryGcsBucket', "dataproc-temp-us-central1-842258736224-teqa50ul")

year = '2021'
df_green = spark.read.parquet(input_green)
df_yellow = spark.read.parquet(input_yellow)

# df_green.printSchema()

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

# df_yellow.printSchema()

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

common_columns = []

yellow_columns = set(df_yellow.columns)

for column in df_green.columns:
    if column in yellow_columns:
        common_columns.append(column)

df_green_serv = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_serv = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

# df_yellow_serv.show(1, vertical=True)

df_trips_data = df_green_serv.unionAll(df_yellow_serv)
# df_trips_data.printSchema()

# df_trips_data.groupBy('service_type').count().show()

df_trips_data.registerTempTable('trips_data')

spark.sql("""
    SELECT service_type, COUNT(service_type) AS count FROM trips_data
    GROUP BY service_type
""").show()

df_result = spark.sql("""
SELECT 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 
    
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance

FROM trips_data
GROUP BY revenue_zone, revenue_month, service_type
""")

# df_result.write.parquet(output, mode="overwrite")

# Save the data to BigQuery
df_result.write.format('bigquery') \
  .option('table', output) \
  .save()


