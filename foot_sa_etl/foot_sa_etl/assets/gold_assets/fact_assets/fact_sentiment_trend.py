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

# load assets reaction and dim_sentiment
# in order to be used as dependency
from foot_sa_etl.assets.gold_assets.fact_assets.fact_reaction import fact_reaction
from foot_sa_etl.assets.gold_assets.fact_assets.fact_title import fact_title


def create_sentiment_trend_table(
        df_fact_reaction: pl.DataFrame,
        df_fact_title: pl.DataFrame,
        df_sentiment: pl.DataFrame,
        df_date: pl.DataFrame) -> pl.DataFrame:

    df_fact_reaction= df_fact_reaction.select(
        [
            'fk_sentiment_id', 'fk_team_id', 'fk_date_id', 'sentiment_score',
            'subjectivity_score', 'is_subjective',
        ]
    )

    df_fact_title= df_fact_title.select(
        [
            'fk_sentiment_id', 'fk_team_id', 'fk_date_id', 'sentiment_score',
            'subjectivity_score', 'is_subjective',
        ]
    )

    df_fact_sentiment_trend = pl.concat([df_fact_reaction, df_fact_title])

    df_date = df_date.select(['date_id', 'year', 'week_of_year'])

    df_fact_sentiment_trend = df_fact_sentiment_trend \
        .join(df_date, left_on='fk_date_id', right_on='date_id', how='left')

    df_fact_sentiment_trend = df_fact_sentiment_trend \
        .join(df_sentiment, left_on='fk_sentiment_id', right_on='sentiment_id', how='left')

    df_fact_sentiment_trend = df_fact_sentiment_trend.with_columns(
        trend_id = pl.concat_str(
            [
                pl.lit('trend'), pl.col('fk_team_id'), pl.col('year'), pl.col('week_of_year')
            ], separator='_'
        )
    )

    # Convert 'is_subjective' to binary (1 for subjective, 0 for objective)
    df_fact_sentiment_trend = df_fact_sentiment_trend.with_columns(
        (pl.col("is_subjective") == "subjective").cast(pl.Int64)
    )

    # Count the sentiment_label occurrences for each trend_id
    df_fact_sentiment_trend = df_fact_sentiment_trend.group_by("trend_id").agg([
        pl.col("fk_team_id").first().alias("fk_team_id"),
        pl.col("fk_date_id").first().alias("fk_date_id"),
        pl.len().alias("total_articles"),  # Count the total number of articles
        (pl.col("is_subjective").mean() * 100).alias("subjectivity_level"),  # Calculate subjectivity level as a percentage
        (pl.col("sentiment_value").mean() * 100).alias("trend_value"),  # Adding trend value as mean sentiment_value
        (pl.col("sentiment_label") == "positive").sum().alias("total_positive_articles"),
        (pl.col("sentiment_label") == "negative").sum().alias("total_negative_articles"),
        (pl.col("sentiment_label") == "neutral").sum().alias("total_neutral_articles")
    ])
    
    return df_fact_sentiment_trend


@asset(
    deps=[fact_reaction, fact_title],
    group_name="epl_sentiment_analysis",
    compute_kind="polars"
)
def fact_sentiment_trend(context: AssetExecutionContext) -> MaterializeResult:
    
    # Create a blob client for Azure Blob Storage
    blob_service_client = create_blob_client_with_connection_string(CONN_STRING_AZURE_STORAGE)
    # List all blobs in the container
    
    gold_container_name = CONFIG['gold_container_name']
    folder_name = CONFIG['folder_name']

    # PROCESSING
    df_fact_reaction = read_blob_from_container(gold_container_name, f"{folder_name}/fact_reaction.parquet", blob_service_client)
    df_fact_title = read_blob_from_container(gold_container_name, f"{folder_name}/fact_title.parquet", blob_service_client)
    df_sentiment = read_blob_from_container(gold_container_name, f"{folder_name}/dim_sentiment.parquet", blob_service_client)
    df_date = read_blob_from_container(gold_container_name, f"{folder_name}/dim_date.parquet", blob_service_client)

    df_fact_sentiment_trend = create_sentiment_trend_table(df_fact_reaction, df_fact_title, df_sentiment, df_date)

    # Define the container and path for the blob storage
    folder_name = CONFIG['folder_name']
    path = f"{folder_name}/df_fact_sentiment_trend.parquet"

    write_blob_to_container(df_fact_sentiment_trend, gold_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_fact_sentiment_trend) # ternary operator
        }
    )
