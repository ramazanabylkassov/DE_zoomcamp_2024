import requests

BASE_API_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

# I call this a paginated getter
# as it's a function that gets data
# and also paginates until there is no more data
# by yielding pages, we "microbatch", which speeds up downstream processing

def paginated_getter():
    page_number = 1

    while True:
        # Set the query parameters
        params = {'page': page_number}

        # Make the GET request to the API
        response = requests.get(BASE_API_URL, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        page_json = response.json()
        print(f'got page number {page_number} with {len(page_json)} records')

        # if the page has no records, stop iterating
        if page_json:
            yield page_json
            page_number += 1
        else:
            # No more data, break the loop
            break

# if __name__ == '__main__':
#     # Use the generator to iterate over pages
#     for page_data in paginated_getter():
#         # Process each page as needed
#         print('hehe')


import dlt

# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')


# we can load any generator to a table at the pipeline destnation as follows:
info = generators_pipeline.run(paginated_getter(),
										table_name="http_download",
										write_disposition="replace")

# the outcome metadata is returned by the load and we can inspect it by printing it.
print(info)

# show outcome

import duckdb

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
print(conn.sql("show tables"))

# and the data

print("\n\n\n http_download table below:")

rides = conn.sql("SELECT * FROM http_download").df()
print(rides)

# As you can see, the same data was loaded in both cases.