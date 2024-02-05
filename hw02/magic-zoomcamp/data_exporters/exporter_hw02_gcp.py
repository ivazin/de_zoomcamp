import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/secret-de-zoomcamp-412119-40c7a977f974.json"
    bucket_name = 'mage-zoomcamp-test-01'
    object_key = 'nyc_taxi_data.parque'
    project_id = 'de-zoomcamp-412119'

    table_name = 'nyc_taxi_data'

    root_path = f'{bucket_name}/{table_name}'

    # data['tpep_pickup_date'] = data['tpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )