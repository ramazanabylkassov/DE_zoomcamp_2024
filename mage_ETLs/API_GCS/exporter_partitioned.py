import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/GC_creds/fresh-ocean-412204-ebb8e1ff44da.json"
bucket_name = 'mage-zoomcamp-ramazan_kaz'
project_id = "fresh-ocean-412204"
table_name = "nyc_taxi_data"
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    data['tpep_pickup_date'] = data['tpep_pickup_datetime'].dt.date
    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()
    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols = ['tpep_pickup_date'],
        filesystem=gcs
    )