import dlt
import duckdb

data = [
    {
        "vendor_name": "VTS",
				"record_hash": "b00361a396177a9cb410ff61f20015ad",
        "time": {
            "pickup": "2009-06-14 23:23:00",
            "dropoff": "2009-06-14 23:48:00"
        },
        "Trip_Distance": 17.52,
        # nested dictionaries could be flattened
        "coordinates": { # coordinates__start__lon
            "start": {
                "lon": -73.787442,
                "lat": 40.641525
            },
            "end": {
                "lon": -73.980072,
                "lat": 40.742963
            }
        },
        "Rate_Code": None,
        "store_and_forward": None,
        "Payment": {
            "type": "Credit",
            "amt": 20.5,
            "surcharge": 0,
            "mta_tax": None,
            "tip": 9,
            "tolls": 4.15,
						"status": "booked"
        },
        "Passenger_Count": 2,
        # nested lists need to be expressed as separate tables
        "passengers": [
            {"name": "John", "rating": 4.9},
            {"name": "Jack", "rating": 3.9}
        ],
        "Stops": [
            {"lon": -73.6, "lat": 40.6},
            {"lon": -73.5, "lat": 40.5}
        ]
    },
    # ... more data
]


# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
pipeline = dlt.pipeline(destination='duckdb', dataset_name='taxi_rides')



# run with merge write disposition.
# This is so scaffolding is created for the next example,
# where we look at merging data

info = pipeline.run(data,
										table_name="rides",
										write_disposition="merge",
                    primary_key="record_hash")

print(info)


# show the outcome

conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
print('Loaded tables: ')
print(conn.sql("show tables"))


print("\n\n\n Rides table below: Note the times are properly typed")
rides = conn.sql("SELECT * FROM rides").df()
print(rides)

print("\n\n\n Pasengers table")
passengers = conn.sql("SELECT * FROM rides__passengers").df()
print(passengers)
print("\n\n\n Stops table")
stops = conn.sql("SELECT * FROM rides__stops").df()
print(stops)


# to reflect the relationships between parent and child rows, let's join them
# of course this will have 4 rows due to the two 1:n joins

print("\n\n\n joined table")

joined = conn.sql("""
SELECT *
FROM rides as r
left join rides__passengers as rp
  on r._dlt_id = rp._dlt_parent_id
left join rides__stops as rs
  on r._dlt_id = rs._dlt_parent_id
""").df()
print(joined[joined.columns[:-4]])