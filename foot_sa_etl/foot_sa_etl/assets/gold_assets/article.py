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
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))

# Config parameters
from utils.config import CONFIG, CONN_STRING_AZURE_STORAGE

# Local project utility imports
from utils.azure_blob_utils import (
    create_blob_client_with_connection_string,
    read_all_parquets_from_container,
    write_blob_to_container
)

# load assets bronze_scrappe_epl_news
# in order to be used as dependency
from foot_sa_etl.assets.silver_assets.process_raw_epl_news import process_raw_epl_news


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

def process_dim_article_table(df):
    """
    Processes a DataFrame to create a dimension table for articles. The function extracts relevant article information, 
    assigns a foreign key 'fk_team_id' by joining with the team dimension, and returns a clean DataFrame with the 
    appropriate columns for article ID, team ID, article title, and publication date.

    :param df: A Polars DataFrame that contains article data with columns such as 'id', 'title', 'publishedDate', and 'teamName'.
    :return: A Polars DataFrame with columns: 'article_id', 'fk_team_id', 'article_title', and 'published_at'.
    """

    # Call get_team_table() to retrieve the unique team dimension table, which includes 'team_id' and 'team_name'
    team_df = process_team_table(df)

    df_processed = df \
        .rename({"id": "article_id"}) \
        .rename({"title": "article_title"}) \
        .rename({"publishedDate": "published_at"}) \
        .rename({"teamName": "team_name"}) \
        .join(team_df, on="team_name") \
        .rename({"team_id": "fk_team_id"}) \
        .select(["article_id", "fk_team_id", "article_title", "published_at"])

    # Return the processed article dimension table
    return df_processed


@asset(
        deps=[process_raw_epl_news],
        group_name="epl_sentiment_analysis",
        compute_kind="polars"
)
def article(context: AssetExecutionContext) -> MaterializeResult:

    # Create a blob client for Azure Blob Storage
    blob_service_client = create_blob_client_with_connection_string(CONN_STRING_AZURE_STORAGE)
    # List all blobs in the container

    silver_container_name = CONFIG['silver_container_name']
    folder_name = CONFIG['folder_name']

    df = read_all_parquets_from_container(silver_container_name, folder_name, blob_service_client)

    df_processed = process_dim_article_table(df)

    # Define the container and path for the blob storage
    gold_container_name = CONFIG['gold_container_name']
    folder_name = CONFIG['folder_name']
    path = f"{folder_name}/article.parquet"

    write_blob_to_container(df_processed, gold_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_processed) # ternary operator
        }
    )
