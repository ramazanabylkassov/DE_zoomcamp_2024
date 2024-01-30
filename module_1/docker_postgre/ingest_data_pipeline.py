#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine 
from time import time
import argparse
import os

def main(params):
    if 'github' in params.url:
        upload_zipped_csv(params)
    else:
        upload_csv(params)

def upload_csv(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'taxi+_zone_lookup.csv'

    os.system(f'curl -o {csv_name} -L {url}')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df = pd.read_csv(f'{csv_name}')
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print('Taxi zone data is uploaded')
    
    os.system(f'rm -rf {csv_name}')

def upload_zipped_csv(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'yellow_tripdata_2019-01.csv'
    # csv_name = 'green_tripdata_2019-09.csv'

    os.system(f'curl -o {csv_name}.gz -L {url}')
    os.system(f'gunzip {csv_name}.gz')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df_iter = pd.read_csv(f'{csv_name}', compression='infer', iterator=True, chunksize = 100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')
    n=1
    total_time = 0.0

    while True:
        try:
            start_time = time()
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists='append')
            end_time = time()
            print(f'Uploading chunk #{n} ({round(end_time - start_time, 3)} sec)')
            n+=1
            total_time = total_time + round(end_time - start_time, 3)
        except StopIteration:
            print(f'''
All data is transferred to PostgreSQL server  
Number of chunks: {n - 1}  
Amount of rows: {(n-1) * 100000} - {(n) * 100000}  
Total time taken: {total_time} sec  
''')
            break

    os.system(f'rm -rf {csv_name}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')
    args = parser.parse_args()
    
    main(args)
