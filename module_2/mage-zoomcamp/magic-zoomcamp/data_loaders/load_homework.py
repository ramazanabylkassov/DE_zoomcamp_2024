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
