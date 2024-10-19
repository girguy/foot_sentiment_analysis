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

from utils.common_helpers import extract_sentiment

# load assets reaction and dim_sentiment
# in order to be used as dependency
from assets.gold_assets.reaction import reaction
from assets.gold_assets.dim_assets.dim_sentiment import dim_sentiment


load_dotenv()

# Get path of the config file
scrapper_config_path = os.path.join(sys.path[-1], 'scrapper_config.json')


def create_fact_reaction(
        df_reaction: pl.DataFrame,
        df_sentiment: pl.DataFrame,
        threshold: float
        ) -> pl.DataFrame:
    """
    Applies sentiment extraction to the reactions in the DataFrame and returns a new Polars DataFrame
    containing the sentiment analysis for each reaction.

    :param df_reaction: A Polars DataFrame containing reaction data with columns 'reaction_id' and 'content'.
    :param threshold: A threshold value to classify neutral sentiment.
    :return: A new Polars DataFrame with sentiment analysis for each reaction.
    """

    # Initialize an empty list to store the sentiment updates
    result_list = []

    # Loop through each row in the df_reaction DataFrame
    for row in df_reaction.iter_rows(named=True):
        # Extract sentiment for each reaction and append the result to the list
        result_list.append(
            extract_sentiment(row['reaction_id'], row['content'], threshold)
        )

    # Convert the sentiment_update_list into a Polars DataFrame
    result_df = pl.DataFrame(
        result_list, 
        schema=["reaction_id", "sentiment_score", "sentiment_label", "subjectivity_score", "is_subjective"],
        orient="row"
    )

    # Return the new Polars DataFrame containing sentiment analysis
    return process_fact_reaction(result_df, df_reaction, df_sentiment)


def process_fact_reaction(
        df_fact_reaction: pl.DataFrame,
        df_reaction: pl.DataFrame,
        df_sentiment: pl.DataFrame
        ) -> pl.DataFrame:
    
    df_fact_reaction = df_fact_reaction.with_columns(
        type = pl.lit('reaction')
    )
    df_fact_reaction = df_fact_reaction.join(df_reaction, on='reaction_id', how='left')
    df_fact_reaction = df_fact_reaction.join(df_sentiment, on='sentiment_label', how='left')

    df_fact_reaction = df_fact_reaction.select(
        [
            'reaction_id', 'fk_article_id', 'sentiment_id', 'fk_team_id',
            'published_at', 'content', 'sentiment_score',
            'subjectivity_score', 'is_subjective', 'is_fan'
        ])

    df_fact_reaction = df_fact_reaction.rename({"sentiment_id": "fk_sentiment_id"})
    df_fact_reaction = df_fact_reaction.rename({"published_at": "fk_date_id"})

    return df_fact_reaction


@asset(
    deps=[reaction, dim_sentiment],
    group_name="epl_sentiment_analysis",
    compute_kind="polars"
)
def fact_reaction(context: AssetExecutionContext) -> MaterializeResult:
    
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

    # PROCESSING
    df_reaction = read_blob_from_container(gold_container_name, f"{folder_name}/reaction.parquet", blob_service_client)
    df_sentiment = read_blob_from_container(gold_container_name, f"{folder_name}/dim_sentiment.parquet", blob_service_client)
    df_fact_reaction = create_fact_reaction(df_reaction, df_sentiment, threshold=0.2)

    # Define the container and path for the blob storage
    folder_name = scrapper_config['folder_name']
    path = f"{folder_name}/fact_reaction.parquet"

    write_blob_to_container(df_fact_reaction, gold_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_fact_reaction) # ternary operator
        }
    )