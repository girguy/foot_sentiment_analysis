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
    read_blob_from_container,
    write_blob_to_container
)

from utils.common_helpers import extract_sentiment

# load assets reaction and dim_sentiment
# in order to be used as dependency
from foot_sa_etl.assets.gold_assets.article import article
from foot_sa_etl.assets.gold_assets.dim_assets.dim_sentiment import dim_sentiment


def create_fact_title(
        df_article: pl.DataFrame,
        df_sentiment: pl.DataFrame,
        threshold: float
        ) -> pl.DataFrame:
    df_article = df_article.with_columns(
        title_id = pl.concat_str(
            [
                pl.col('article_id'),
                pl.lit('title')
            ],
            separator='_'
        )
    )

    # Initialize an empty list to store the sentiment updates
    result_list = []

    # Loop through each row in the df_article DataFrame
    for row in df_article.iter_rows(named=True):
        # Extract sentiment for each title and append the result to the list
        result_list.append(
            extract_sentiment(row['title_id'], row['article_title'], threshold)
        )

    # Convert the sentiment_update_list into a Polars DataFrame
    result_df = pl.DataFrame(
        result_list, 
        schema=["title_id", "sentiment_score", "sentiment_label", "subjectivity_score", "is_subjective"],
        orient="row"
    )

    return process_title_reaction(result_df, df_article, df_sentiment)

def process_title_reaction(
        df_fact_title: pl.DataFrame,
        df_article: pl.DataFrame,
        df_sentiment: pl.DataFrame
        ) -> pl.DataFrame:

    # Return the new Polars DataFrame containing sentiment analysis
    df_fact_title = df_fact_title.with_columns(
        type = pl.lit('title')
    )
    df_fact_title = df_fact_title.join(df_article, on='title_id', how='left')
    df_fact_title = df_fact_title.join(df_sentiment, on='sentiment_label', how='left') ###

    df_fact_title = df_fact_title.select(
        [
            'title_id', 'sentiment_id', 'fk_team_id',
            'published_at', 'article_title', 'sentiment_score',
            'subjectivity_score', 'is_subjective', 'type'
        ]
    )

    df_fact_title = df_fact_title.rename({"sentiment_id": "fk_sentiment_id"})
    df_fact_title = df_fact_title.rename({"published_at": "fk_date_id"})
    df_fact_title = df_fact_title.rename({"article_title": "title"})

    return df_fact_title


@asset(
    deps=[article, dim_sentiment],
    group_name="epl_sentiment_analysis",
    compute_kind="polars"
)
def fact_title(context: AssetExecutionContext) -> MaterializeResult:

    # Create a blob client for Azure Blob Storage
    blob_service_client = create_blob_client_with_connection_string(CONN_STRING_AZURE_STORAGE)
    # List all blobs in the container
    
    gold_container_name = CONFIG['gold_container_name']
    folder_name = CONFIG['folder_name']

    # PROCESSING
    df_article = read_blob_from_container(gold_container_name, f"{folder_name}/article.parquet", blob_service_client)
    df_sentiment = read_blob_from_container(gold_container_name, f"{folder_name}/dim_sentiment.parquet", blob_service_client)
    df_fact_title = create_fact_title(df_article, df_sentiment, threshold=0.2)

    # Define the container and path for the blob storage
    folder_name = CONFIG['folder_name']
    path = f"{folder_name}/fact_title.parquet"

    write_blob_to_container(df_fact_title, gold_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_fact_title) # ternary operator
        }
    )