import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    urls = [
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet'
    ]

    # taxi_dtypes = {
    #                 'VendorID': pd.Int64Dtype(),
    #                 'passenger_count': pd.Int64Dtype(),
    #                 'trip_distance': float,
    #                 'RatecodeID':pd.Int64Dtype(),
    #                 'store_and_fwd_flag':str,
    #                 'PULocationID':pd.Int64Dtype(),
    #                 'DOLocationID':pd.Int64Dtype(),
    #                 'payment_type': pd.Int64Dtype(),
    #                 'fare_amount': float,
    #                 'extra':float,
    #                 'mta_tax':float,
    #                 'tip_amount':float,
    #                 'tolls_amount':float,
    #                 'improvement_surcharge':float,
    #                 'total_amount':float,
    #                 'congestion_surcharge':float
    #             }

    # # native date parsing 
    # parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    output_df = pd.DataFrame()
    for url in urls:
        output_df = pd.concat([output_df, pd.read_parquet(url)])
        print(f"batch processed {output_df.shape[0]}")

    return output_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
