# Authenticate to Google BigQuery
from google.colab import auth
auth.authenticate_user()

import requests
import json

url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"
# If this file were json and not jsonl, we could use ijson library to break it into lines without loading to memory.

def stream_download_jsonl(url):
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    for line in response.iter_lines():
        if line:
            yield json.loads(line)

# time the download
import time
start = time.time()

# Use the generator to iterate over rows with minimal memory usage
row_counter = 0
for row in stream_download_jsonl(url):
    print(row)
    row_counter += 1
    if row_counter >= 5:
        break

# time the download
end = time.time()
print(end - start)