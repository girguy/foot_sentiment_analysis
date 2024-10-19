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
    read_blob_from_container, 
    write_blob_to_container
)

# load assets bronze_scrappe_epl_news
# in order to be used as dependency
from assets.gold_assets.article import article


load_dotenv()

# Get path of the config file
scrapper_config_path = os.path.join(sys.path[-1], 'scrapper_config.json')


def process_article_table(df):
    df_processed = df.with_columns(
        fk_title_id = pl.concat_str(
            [
                pl.col('article_id'),
                pl.lit('title')
            ],
            separator='_'
        )
    )
    return df_processed



@asset(
        deps=[article],
        group_name="epl_sentiment_analysis",
        compute_kind="polars"
)
def dim_article(context: AssetExecutionContext) -> MaterializeResult:
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

    gold_container_name = scrapper_config['gold_container_name']
    folder_name = scrapper_config['folder_name']

    df_article = read_blob_from_container(gold_container_name, f"{folder_name}/article.parquet", blob_service_client)

    df_dim_article = process_article_table(df_article)

    # Define the container and path for the blob storage
    gold_container_name = scrapper_config['gold_container_name']
    folder_name = scrapper_config['folder_name']
    path = f"{folder_name}/dim_article.parquet"

    write_blob_to_container(df_dim_article, gold_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_dim_article) # ternary operator
        }
    )