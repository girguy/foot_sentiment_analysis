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
    read_all_parquets_from_container, 
    write_blob_to_container
)
from utils.common_helpers import generate_hash

# load assets bronze_scrappe_epl_news
# in order to be used as dependency
from assets.silver_assets.process_raw_epl_news import process_raw_epl_news


load_dotenv()

# Get path of the config file
scrapper_config_path = os.path.join(sys.path[-1], 'scrapper_config.json')


def process_team_table(df):
    """
    Processes a DataFrame to create a unique team dimension table. The function extracts unique team names 
    from the 'teamName' column, assigns each team a unique ID, and returns a DataFrame with 'team_id' and 
    'team_name' columns, sorted by team name.

    :param df: A Polars DataFrame that contains a 'teamName' column with team names.
    :return: A new Polars DataFrame with two columns: 'team_id' (a unique identifier for each team) and 'team_name'.
    """

    # Select unique team names from the DataFrame
    df_selected = df.select(pl.col("teamName")).unique()

    # Rename the 'teamName' column to 'team_name' for consistent naming conventions
    df_selected = df_selected.rename({"teamName": "team_name"}).sort(by='team_name')

    # Add a 'team_id' column that assigns a unique ID to each team, starting from 1
    df_selected = df_selected.with_columns(
        team_id=range(1, df_selected.shape[0] + 1)  # Generates a sequential range of IDs
    )

    # Return a DataFrame with only 'team_id' and 'team_name', ensuring it's sorted by team name
    return df_selected.select(['team_id', 'team_name'])



@asset(
        deps=[process_raw_epl_news],
        group_name="epl_sentiment_analysis",
        compute_kind="polars"
)
def dim_team(context: AssetExecutionContext) -> MaterializeResult:
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

    df = read_all_parquets_from_container(silver_container_name, folder_name, blob_service_client)

    df_processed = process_team_table(df)

    # Define the container and path for the blob storage
    gold_container_name = scrapper_config['gold_container_name']
    folder_name = scrapper_config['folder_name']
    path = f"{folder_name}/dim_team.parquet"

    write_blob_to_container(df_processed, gold_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_processed) # ternary operator
        }
    )