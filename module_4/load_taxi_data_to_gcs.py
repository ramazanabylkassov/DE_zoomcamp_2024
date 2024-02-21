import pandas as pd
import os
import dlt

# Set the bucket_url. We can also use a local folder
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/ramazan/DE_zoomcamp_2024/module_2/mage-zoomcamp/GC_creds/fresh-ocean-412204-ebb8e1ff44da.json"
os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = 'gs://dlt_workshop'

# Define your pipeline
pipeline = dlt.pipeline(
    pipeline_name='my_pipeline',
    destination='filesystem',
    dataset_name='mydata'
)

def load_data_from_api(*args, **kwargs):
    months = [
        '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'
    ]
    years = ['2019']
    urls = []

    for year in years:        
        for month in months:
            urls.append(f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month}.csv.gz')
    
    print('here you go')

    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type': pd.Int64Dtype(),
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'congestion_surcharge':float
                }

    # native date parsing 
    parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    for url in urls:
        yield pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        print(f"Batch processed: {url[-29:-7]}")

load_info = pipeline.run(
    load_data_from_api(),
    table_name="users", 
    write_disposition="append",
    loader_file_format="parquet")

