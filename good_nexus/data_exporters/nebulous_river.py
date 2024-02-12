from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
from io import BytesIO
from google.cloud import storage


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
# Convert the DataFrame to a Parquet file-like object
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    # Initialize the GCS client
    client = storage.Client()
    bucket = client.bucket('yemisi-mage')

    # Create a blob (object) in the specified bucket
    blob = bucket.blob('green_taxi_data.parquet')

    # Set the position of the buffer to the beginning
    parquet_buffer.seek(0)

    # Upload the Parquet data
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
