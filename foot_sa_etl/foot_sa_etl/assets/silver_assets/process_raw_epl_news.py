# Standard library imports
import os
import sys
import json
from datetime import datetime
from typing import Optional

# Third-party library imports
from bs4 import BeautifulSoup
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
    write_blob_to_container, 
    read_blob_from_container, 
    merge_dataframes_on_id
)
from utils.common_helpers import generate_hash

# load assets bronze_scrappe_epl_news
# in order to be used as dependency
from assets.bronze_assets.scrappe_epl_news import bronze_scrappe_epl_news


load_dotenv()

# Get path of the config file
scrapper_config_path = os.path.join(sys.path[-1], 'scrapper_config.json')



def extract_html_fields(html: str) -> tuple:
    """
    Extracts 'publishedDate', 'title', and 'content' from the given HTML string.

    :param html: The HTML content as a string
    :return: A tuple containing (publishedDate, title, content)
    """
    soup = BeautifulSoup(html, 'html.parser')

    # Find all articles based on the class
    articles = soup.find_all('span', class_='ssrcss-189b1h2-HeadlineWrap')
    
    # Default values
    published_date = "No date found"
    title = "No title found"
    full_text = "No content found"
    
    if articles:
        for article in articles:
            # Extract title
            title = article.find('span').get_text() if article.find('span') else "No title found"
            
            # Extract published date
            timestamp = article.find('span', {'data-testid': 'timestamp'})
            if timestamp:
                published_date = timestamp.find('span', {'data-testid': 'accessible-timestamp'}).get_text()
            else:
                published_date = "No date found"
            
            # Extract all paragraphs related to the article
            text_content = []
            parent_article = article.find_parent('article')
            if parent_article:
                paragraphs = parent_article.find_all('p', class_='ssrcss-1q0x1qg-Paragraph e1jhz7w10')
                text_content = [p.get_text() for p in paragraphs]

            full_text = ' '.join(text_content) if text_content else "No content found"

    # Return as a dictionary (which Polars can convert to a Struct)
    return {
        "publishedDate": published_date,
        "title": title,
        "content": full_text
    }


def parse_published_date(date_str: str) -> str:
    """
    Parses a published date string like 'published at 22:46 2 October' and converts
    it to a 'YYYY-MM-DD HH:MM:SS' format.
    
    :param date_str: The date string to be parsed
    :return: A formatted date string
    """
    try:
        # Remove the 'published at' prefix and split the time and date
        date_str = date_str.replace('published at ', '').strip()
        time_part, day_part, month_part = date_str.split()

        # Current year (assumed, can be replaced with logic to determine the correct year)
        current_year = datetime.now().year

        # Convert the string into a datetime object
        datetime_obj = datetime.strptime(f"{time_part} {day_part} {month_part} {current_year}", "%H:%M %d %B %Y")

        # Return the date in the desired format
        return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Error parsing date: {date_str}, Error: {e}")
        return None  # or handle the error as needed

def process_html_column(df: pl.DataFrame) -> pl.DataFrame:
    """
    Optimized version of processing the 'html' column in the Polars DataFrame. This
    function calls the HTML processing function only once per row and extracts
    'publishedDate', 'title', and 'content' in a single operation.

    :param df: Input Polars DataFrame containing an 'html' column
    :return: A new DataFrame with 'publishedDate', 'title', and 'content' columns
    """
    # Apply the extraction function once and unpack the result into three new columns
    new_columns = df.select(
        pl.col("html").map_elements(extract_html_fields, return_dtype=pl.Struct([
            pl.Field("publishedDate", pl.Utf8),
            pl.Field("title", pl.Utf8),
            pl.Field("content", pl.Utf8)
        ])).alias("extracted")
    ).unnest("extracted")

    # Parse the publishedDate column and convert it to a proper date format
    new_columns = new_columns.with_columns(
        pl.col("publishedDate").map_elements(parse_published_date, return_dtype=pl.Utf8).alias("publishedDate")
    )

    df = df.with_columns(new_columns)

    columns_to_keep = ["teamName", "publishedDate", "title", "content"]
    
    return df.select(columns_to_keep)


def filter_unwanted_titles(df: pl.DataFrame) -> pl.DataFrame:
    """
    Filters out rows in a Polars DataFrame where the 'title' column contains unwanted patterns.
    These patterns correspond to specific phrases or structures found in sports articles that should be excluded.

    :param df: A Polars DataFrame containing a 'title' column to filter.
    :return: A Polars DataFrame with the unwanted rows filtered out.
    """

    # Define regex patterns to match unwanted strings in the 'title' column
    patterns_to_filter = [
        r"Catch up on the Premier League action",
        r"Follow (Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)'s (Premier League games|Carabao Cup)",
        r"Follow\s+([A-Za-z\s]+)\s+v\s+([A-Za-z\s]+)",
        r"who is your team facing\?",
        r"send us your thoughts"
    ]

    # Combine the patterns into a single regex expression with the OR operator (|)
    combined_pattern = "|".join(patterns_to_filter)

    # Filter out rows where the 'title' column contains any of the unwanted patterns
    df_filtered = df.filter(~pl.col("title").str.contains(combined_pattern))

    return df_filtered



@asset(
        deps=[bronze_scrappe_epl_news],
        group_name="epl_sentiment_analysis",
        compute_kind="polars"
)
def silver_process_raw_epl_news(context: AssetExecutionContext) -> MaterializeResult:
    """
    This function processes scraped EPL news data stored in Azure Blob Storage. 
    It reads Parquet files, processes HTML content, generates a unique ID, and uploads the processed data 
    to a new container in Azure Blob Storage.

    :param context: The context object provided by Dagster to log and track asset execution.
    """

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

    bronze_container_name = scrapper_config['bronze_container_name']
    folder_name = scrapper_config['folder_name']

    df = read_all_parquets_from_container(bronze_container_name, folder_name, blob_service_client)

    df_processed = process_html_column(df)

    #  create primary key based on columns teamName
    #  publishedDate and title
    df_processed = df_processed.with_columns(
        pl.concat_str(
            [
                pl.col("teamName"),
                pl.col("publishedDate"),
                pl.col("title")
            ]
        ).alias("id")
    )

    # hash the the column id
    df_processed = df_processed.with_columns(
            pl.col("id").map_elements(generate_hash, return_dtype=pl.String).alias("id")
        )

    # slice the "id" column
    df_processed = df_processed.with_columns(
            pl.col("id").str.slice(0, 16)
        )

    # Keep unique rows based on column "id"
    df_processed = df_processed.unique(subset="id")

    # clean df_processed by removing some of the records based on title
    df_processed = filter_unwanted_titles(df_processed)

    # Define the container and path for the blob storage
    silver_container_name = scrapper_config['silver_container_name']
    silver_blob_name = scrapper_config['silver_blob_name']
    folder_name = scrapper_config['folder_name']
    path = f"{folder_name}/{silver_blob_name}.parquet"

    # Read the existing blob data from Azure Blob Storage, if available
    df_actual: Optional[pl.DataFrame] = read_blob_from_container(silver_container_name, path, blob_service_client)

    if df_actual is None:
        # If no existing data, write the new DataFrame directly to the blob
        print("No existing data found, writing new data to blob...")
        write_blob_to_container(df_processed, silver_container_name, path, blob_service_client)
        df_merged = None # for ternary operator
    else:
        # If existing data is found, merge it with the new data
        print("Existing data found, merging with new data...")
        df_merged = merge_dataframes_on_id(df_actual, df_processed, "id")

        # Write the merged DataFrame back to the blob
        write_blob_to_container(df_merged, silver_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_merged if df_merged is not None else df_processed) # ternary operator
        }
    )