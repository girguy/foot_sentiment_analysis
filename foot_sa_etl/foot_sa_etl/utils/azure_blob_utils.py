import re
from typing import Union
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import polars as pl


def create_blob_client_with_connection_string(connection_string: str) -> BlobServiceClient:
    """
    Creates a BlobServiceClient using a connection string, handling URL-encoded characters.

    :param connection_string: The Azure Storage connection string
    :return: BlobServiceClient object
    """
    connection_string = re.sub(r'%2B', '+', connection_string)
    return BlobServiceClient.from_connection_string(connection_string)


def write_blob_to_container(df: pl.DataFrame, container_name: str, path_to_blob: str, blob_service_client: BlobServiceClient) -> None:
    """
    Writes a Polars DataFrame as a Parquet file to an Azure Blob Storage container.

    :param df: Polars DataFrame to write
    :param container_name: Name of the Azure Blob Storage container
    :param path_to_blob: Path to the blob in the container
    :param blob_service_client: BlobServiceClient object for Azure Blob Storage
    """
    parquet_buffer = from_polars_to_parquet(df)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=path_to_blob)
    try:
        blob_client.upload_blob(parquet_buffer.getvalue(), blob_type="BlockBlob", overwrite=True)
        print(f"Successfully uploaded blob to {container_name}/{path_to_blob}")
    except Exception as e:
        print(f"Error uploading blob to {container_name}/{path_to_blob}: {e}")


def read_blob_from_container(container_name: str, path_to_blob: str, blob_service_client: BlobServiceClient) -> Union[pl.DataFrame, None]:
    """
    Reads a Parquet file from an Azure Blob Storage container and returns it as a Polars DataFrame.

    :param container_name: Name of the Azure Blob Storage container
    :param path_to_blob: Path to the blob in the container
    :param blob_service_client: BlobServiceClient object for Azure Blob Storage
    :return: Polars DataFrame read from the blob, or None if the operation fails
    """
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=path_to_blob)
    try:
        download_stream = blob_client.download_blob()
        blob_data = download_stream.readall()
        df = pl.read_parquet(BytesIO(blob_data))
        print(f"Successfully read blob from {container_name}/{path_to_blob}")
        return df
    except Exception as e:
        print(f"Error reading blob from {container_name}/{path_to_blob}: {e}")
        return None


def from_polars_to_parquet(df: pl.DataFrame) -> BytesIO:
    """
    Converts a Polars DataFrame to a Parquet file in memory.

    :param df: Polars DataFrame to be converted to Parquet
    :return: BytesIO buffer containing the Parquet file
    """
    parquet_buffer = BytesIO()
    df.write_parquet(parquet_buffer, use_pyarrow=True)
    parquet_buffer.seek(0)  # Reset buffer position to the beginning
    return parquet_buffer
