# Standard library imports
import os
import sys

# Third-party library imports
import polars as pl

# Dagster imports
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    asset
)

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../..')))

# Config parameters
from utils.config import CONFIG, CONN_STRING_AZURE_STORAGE

# Local project utility imports
from utils.azure_blob_utils import (
    create_blob_client_with_connection_string,
    write_blob_to_container
)


def create_sentiment_table():
    dict_sentiment = {
        'sentiment_label': ['negative', 'neutral', 'positive'],
        'sentiment_id': [0, 1, 3],
        'sentiment_value': [-1, 0, 1]
    }

    return pl.from_dict(dict_sentiment)


@asset(
        group_name="epl_sentiment_analysis",
        compute_kind="polars"
)
def dim_sentiment(context: AssetExecutionContext) -> MaterializeResult:

    # Create a blob client for Azure Blob Storage
    blob_service_client = create_blob_client_with_connection_string(CONN_STRING_AZURE_STORAGE)
    # List all blobs in the container

    folder_name = CONFIG['folder_name']

    df_sentiment = create_sentiment_table()

    # Define the container and path for the blob storage
    gold_container_name = CONFIG['gold_container_name']
    folder_name = CONFIG['folder_name']
    path = f"{folder_name}/dim_sentiment.parquet"

    write_blob_to_container(df_sentiment, gold_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_sentiment) # ternary operator
        }
    )