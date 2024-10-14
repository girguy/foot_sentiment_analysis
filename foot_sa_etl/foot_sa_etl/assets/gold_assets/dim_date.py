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
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

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


def process_dim_date_table(df):
    """
    Processes a DataFrame and extracts date-related information from the 'publishedDate' column 
    to create a Date Dimension table. The resulting table contains the date, year, month, day, 
    and week of the year, which can be used for date-based analysis or to populate a Date Dimension 
    table in a star schema.

    :param df: A Polars DataFrame that contains a 'publishedDate' column, which should be a date or timestamp.
    :return: A new Polars DataFrame with date-related columns: 'date', 'year', 'month', 'day', and 'week_of_year'.
    """

    # Extract the 'publishedDate' column as a Series for date operations
    date_series = df.select('publishedDate').to_series()

    # Create the Date Dimension Table by extracting relevant date components
    date_dim = pl.DataFrame({
        "date": date_series,                   # Original date column
        "year": date_series.dt.year(),         # Extract the year from the date
        "month": date_series.dt.month(),       # Extract the month from the date
        "day": date_series.dt.day(),           # Extract the day from the date
        "week_of_year": date_series.dt.week()  # Extract the week of the year (ISO week 1 to 53)
    })

    # Return the newly created Date Dimension table
    return date_dim



@asset(
        deps=[process_raw_epl_news],
        group_name="epl_sentiment_analysis",
        compute_kind="polars"
)
def dim_date(context: AssetExecutionContext) -> MaterializeResult:
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

    df_processed = process_dim_date_table(df)

    # Define the container and path for the blob storage
    gold_container_name = scrapper_config['gold_container_name']
    folder_name = scrapper_config['folder_name']
    path = f"{folder_name}/dim_date.parquet"

    write_blob_to_container(df_processed, gold_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_processed) # ternary operator
        }
    )