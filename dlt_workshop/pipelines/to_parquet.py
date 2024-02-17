import os
import dlt
import glob
import requests
import json

# Set the bucket_url. We can also use a local folder
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/ramazan/DE_zoomcamp_2024/module_2/mage-zoomcamp/GC_creds/fresh-ocean-412204-ebb8e1ff44da.json"
os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = 'gs://dlt_workshop'

url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"
# Define your pipeline
pipeline = dlt.pipeline(
    pipeline_name='my_pipeline',
    destination='filesystem',
    dataset_name='mydata'
)

def stream_download_jsonl(url):
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    for line in response.iter_lines():
        if line:
            yield json.loads(line)

# Run the pipeline with the generator we created earlier.
load_info = pipeline.run(stream_download_jsonl(url), table_name="users", loader_file_format="parquet")

print(load_info)

# Get a list of all Parquet files in the specified folder
parquet_files = glob.glob('/dlt_workshop/mydata/users/*.parquet')

# show parquet files
print("Loaded files: ")
for file in parquet_files:
  print('hehe')
