import sys
import os
import re
from typing import Union, List
from io import BytesIO
import polars as pl

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from azure.storage.blob import BlobServiceClient


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


def read_all_parquets_from_container(container_name: str, folder_name: str, blob_service_client: BlobServiceClient) -> Union[List[pl.DataFrame], None]:
    """
    Reads all Parquet files from an Azure Blob Storage container and returns them as a list of Polars DataFrames.

    :param container_name: Name of the Azure Blob Storage container
    :param blob_service_client: BlobServiceClient object for Azure Blob Storage
    :return: List of Polars DataFrames, or None if the operation fails
    """
    container_client = blob_service_client.get_container_client(container_name)
    dataframes = []

    try:
        # List all blobs in the container
        blob_list = container_client.list_blobs()

        # Iterate over the blobs and read Parquet files
        for blob in blob_list:
            if blob.name.endswith('.parquet') and blob.name.startswith(folder_name):  # Process only parquet files
                blob_client = container_client.get_blob_client(blob.name)
                download_stream = blob_client.download_blob()
                blob_data = download_stream.readall()

                # Read the blob data into a Polars DataFrame
                df = pl.read_parquet(BytesIO(blob_data))
                dataframes.append(df)
                print(f"Successfully read parquet file from {container_name}/{blob.name}")

        if dataframes:
            return pl.concat(dataframes, rechunk=True)
        else:
            return None

    except Exception as e:
        print(f"Error reading Parquet files from container {container_name}: {e}")
        return None


def merge_dataframes_on_id(df1: pl.DataFrame, df2: pl.DataFrame, col_id: pl.String) -> pl.DataFrame:
    """
    Merges two Polars DataFrames based on the col_id column.
    If an col_id from df1 exists in df2, it will not be added.

    :param df1: First Polars DataFrame
    :param df2: Second Polars DataFrame
    :return: Merged Polars DataFrame with no duplicate col_id records from df1
    """
    try:
        # Perform an anti-join to find records in df1 that do not have a match in df2 based on col_id
        df_filtered = df1.join(df2, on=col_id, how="anti")

        # Log the result of the anti-join operation
        if df_filtered.is_empty():
            print("No new records to merge.")
        else:
            print(f"{df_filtered.shape[0]} new records found. Merging them.")

        # Concatenate the filtered rows from df1 with df2
        df_merged = pl.concat([df2, df_filtered], how="vertical")
        return df_merged

    except Exception as e:
        print(f"Error occurred while merging dataframes: {e}")
        raise


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
