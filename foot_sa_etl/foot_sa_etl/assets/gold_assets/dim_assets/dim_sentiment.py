# Standard library imports
import os
import sys
import json

# Third-party library imports
from dotenv import load_dotenv
import polars as pl

# Dagster imports
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    asset
)

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Local project utility imports
from utils.azure_blob_utils import (
    create_blob_client_with_connection_string, 
    write_blob_to_container
)


load_dotenv()

# Get path of the config file
scrapper_config_path = os.path.join(sys.path[-1], 'scrapper_config.json')



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
    # Load the JSON file
    with open(scrapper_config_path, 'r') as file:
        scrapper_config = json.load(file)

    # Load environment variables
    connection_string = os.environ.get("CONN_STRING_AZURE_STORAGE")
    if connection_string is None:
        raise EnvironmentError("Azure storage connection string not found in environment variables.")

    # Create a blob client for Azure Blob Storage
    blob_service_client = create_blob_client_with_connection_string(connection_string)
    # List all blobs in the container

    silver_container_name = scrapper_config['silver_container_name']
    folder_name = scrapper_config['folder_name']

    df_sentiment = create_sentiment_table()

    # Define the container and path for the blob storage
    gold_container_name = scrapper_config['gold_container_name']
    folder_name = scrapper_config['folder_name']
    path = f"{folder_name}/dim_sentiment.parquet"

    write_blob_to_container(df_sentiment, gold_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_sentiment) # ternary operator
        }
    )