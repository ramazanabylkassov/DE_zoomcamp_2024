from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
import os
import pandas_gbq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/GC_creds/fresh-ocean-412204-ebb8e1ff44da.json"
project_id = "fresh-ocean-412204"
dataset_id = "ny_taxi_part"  # Change this to your dataset ID
table_name = "yellow_cab_data"


@data_exporter
def export_data_to_bigquery(data, partition_column='tpep_pickup_datetime'):
    pandas_gbq.to_gbq(data, f'{project_id}.{dataset_id}.{table_name}', 
                      project_id=project_id, 
                      if_exists='append', 
                      partition_column=partition_column)
