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



def create_type_table():
    dict_type = {
        'type_id': [0, 1],
        'type': ['reaction', 'title'],
    }

    return pl.from_dict(dict_type)


@asset(
        group_name="epl_sentiment_analysis",
        compute_kind="polars"
)
def dim_type(context: AssetExecutionContext) -> MaterializeResult:
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

    folder_name = scrapper_config['folder_name']

    df_type = create_type_table()

    # Define the container and path for the blob storage
    gold_container_name = scrapper_config['gold_container_name']
    folder_name = scrapper_config['folder_name']
    path = f"{folder_name}/dim_type.parquet"

    write_blob_to_container(df_type, gold_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_type) # ternary operator
        }
    )