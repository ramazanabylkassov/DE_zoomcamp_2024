import requests
import json


def download_and_read_jsonl(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    data = response.text.splitlines()
    parsed_data = [json.loads(line) for line in data]
    return parsed_data


# time the download
import time
start = time.time()

url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"
downloaded_data = download_and_read_jsonl(url)

if downloaded_data:
    # Process or print the downloaded data as needed
    print(downloaded_data[:5])  # Print the first 5 entries as an example

# time the download
end = time.time()
print(end - start)