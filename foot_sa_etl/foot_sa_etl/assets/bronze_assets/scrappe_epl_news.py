# Standard library imports
import os
import sys
import json
import asyncio
from typing import List, Tuple, Union, Optional

# Third-party imports
import aiohttp
import polars as pl
from dotenv import load_dotenv

# Dagster imports
from dagster import AssetExecutionContext, MaterializeResult, asset

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Local project utility imports
from utils.azure_blob_utils import (
    create_blob_client_with_connection_string,
    write_blob_to_container,
    read_blob_from_container,
    merge_dataframes_on_id
)
from utils.common_helpers import get_current_datetime, generate_hash, create_blob_name


load_dotenv()

# Get path of the config file
scrapper_config_path = os.path.join(sys.path[-1], 'scrapper_config.json')


def get_teams_url(epl_teams, number_of_pages, base_url: str) -> List[List[Union[str, int, str]]]:
    """
    Generates a list of URLs for the given teams and the specified number of pages.

    :param epl_teams: A dictionary of team names and their corresponding values
    :param number_of_pages: The number of pages to scrape for each team
    :param base_url: The base url of the website to scrap
    :return: A list of lists containing team name, page number, and URL
    """
    # Dictionary to hold the URLs
    team_urls = []

    for team_name, value in epl_teams.items():
        for page_number in range(1, number_of_pages + 1):
            # Create a list of URLs for each team
            url = f"{base_url}/{value}/?page={page_number}"
            team_urls.append([team_name, page_number, url])

    return team_urls


def create_dataframe(input_df: List[List[Union[str, int, str]]], datetime_now: str) -> pl.DataFrame:
    """
    Creates a Polars DataFrame with the specified schema, adds the current datetime to each row,
    and generates a hash of the HTML content.

    :param input_df: Input data as a list of lists
    :param datetime_now: Current datetime as a string to add to the "_extractedDate" column
    :return: A Polars DataFrame with the specified schema and new columns
    """
    # Define the schema explicitly
    schema = pl.Schema({
        "teamName": pl.String(),
        "page": pl.Int8,
        "html": pl.String()
    })

    df = pl.from_records(
        input_df,
        schema=schema,
        orient='row'
    )

    # Create the "_extractedDate" column using the provided datetime
    date_list = [datetime_now for i in range(len(input_df))]
    date_series = pl.Series("_extractedDate", date_list)
    datetime_series = date_series.str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M:%S')
    df = df.with_columns(
        pl.Series("_extractedDate", datetime_series)
    )

    # Add a new "_hashedId" column by applying the generate_hash function to the "html" column
    df = df.with_columns(
        pl.col("html").map_elements(lambda x: generate_hash(x), return_dtype=pl.Utf8).alias("_hashedId")
    )

    # Rearrange the columns
    return df.select(["_hashedId", "_extractedDate", "teamName", "page", "html"])


async def get_page(team_name: str, page_number: int, url: str, session: aiohttp.ClientSession) -> List[str]:
    """
    Fetches the HTML content of a webpage asynchronously with retries in case of failure.

    :param team_name: Name of the team
    :param page_number: Page number to fetch
    :param url: The URL to request
    :param session: An aiohttp ClientSession object for making requests
    :return: A list containing the team name, page number, and either the HTML content or an error message
    """
    retries = 3
    backoff_factor = 2
    delay = 1

    for attempt in range(retries):
        try:
            async with session.get(url) as response:
                if 200 <= response.status < 300:
                    print(f"Success: {team_name} Page {page_number}")
                    return [team_name, page_number, await response.text()]
                elif response.status == 429:  # too many requests code
                    print(f"Error {response.status} for {url}, retrying for id {id}...")
                    raise aiohttp.ClientError(f"HTTP error 429 for {url}")
                else:
                    print(f"Failed with status {response.status} for {team_name}, page {page_number}")
                    return [team_name, page_number, f"HTTP error {response.status}"]
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Error {e}, retrying in {delay} seconds for {team_name}, page {page_number} (Attempt {attempt + 1}/{retries})")
            await asyncio.sleep(delay)
            delay *= backoff_factor  # Increase the delay exponentially

        # After max retries, raise the exception
        if attempt == retries - 1:
            print(f"Failed after {retries} attempts for {team_name}, page {page_number}")
            return [team_name, page_number, f"Failed after {retries} attempts"]


async def get_all_pages(teams_details: List[Tuple[str, int, str]], session: aiohttp.ClientSession) -> List[List[str]]:
    """
    Creates asynchronous tasks to fetch pages for all teams and collects the results.

    :param teams_details: A list of tuples where each tuple contains (team_name, page_number, url)
    :param session: An aiohttp ClientSession object for making requests
    :return: A list of lists containing the team name, page number, and page content or error messages
    """
    tasks = [asyncio.create_task(get_page(team_name, page_number, url, session))
             for team_name, page_number, url in teams_details]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    return results


async def scrapper(urls: List[Tuple[str, int, str]]) -> List[List[str]]:
    """
    Main asynchronous entry point that sets up the aiohttp session and fetches pages for all URLs.

    :param urls: A list of tuples where each tuple contains (team_name, page_number, url)
    :return: A list of lists containing the team name, page number, and page content or error messages
    """
    async with aiohttp.ClientSession() as session:
        data = await get_all_pages(urls, session)
    return data


@asset(group_name="epl_sentiment_analysis", compute_kind="polars")
def bronze_scrappe_epl_news(context: AssetExecutionContext) -> MaterializeResult:
    """
    This function scrapes EPL team news from BBC Sport and stores the data in Azure Blob Storage
    as a Parquet file. If existing data is found in the blob, it merges the new data with the old data.

    Parameters:
    - context (AssetExecutionContext): The execution context for the Dagster asset.

    Returns:
    - None
    """

    # Load the JSON file
    with open(scrapper_config_path, 'r') as file:
        scrapper_config = json.load(file)

    # Get the list of team URLs to scrape
    team_urls = get_teams_url(
        scrapper_config['teams'],
        scrapper_config['nb_page'],
        scrapper_config['base_url']
    )

    # Scrape the data from the URLs asynchronously
    results = asyncio.run(scrapper(team_urls))

    # Get the current datetime
    datetime_now = get_current_datetime()

    # Create a new Polars DataFrame from the scraped results
    df_new = create_dataframe(results, datetime_now)

    # Load environment variables
    connection_string = os.environ.get("CONN_STRING_AZURE_STORAGE")
    if connection_string is None:
        raise EnvironmentError("Azure storage connection string not found in environment variables.")

    # Create a blob client for Azure Blob Storage
    blob_service_client = create_blob_client_with_connection_string(connection_string)

    # Define the container and path for the blob storage
    bronze_container_name = scrapper_config['bronze_container_name']
    folder_name = scrapper_config['folder_name']
    blob_name = create_blob_name(datetime_now)
    path = f"{folder_name}/{blob_name}.parquet"

    # Read the existing blob data from Azure Blob Storage, if available
    df_actual: Optional[pl.DataFrame] = read_blob_from_container(bronze_container_name, path, blob_service_client)

    if df_actual is None:
        # If no existing data, write the new DataFrame directly to the blob
        print("No existing data found, writing new data to blob...")
        write_blob_to_container(df_new, bronze_container_name, path, blob_service_client)
        df_merged = None # For the ternary operator
    else:
        # If existing data is found, merge it with the new data
        print("Existing data found, merging with new data...")
        df_merged = merge_dataframes_on_id(df_actual, df_new, "_hashedId")

        # Write the merged DataFrame back to the blob
        write_blob_to_container(df_merged, bronze_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_merged if df_merged is not None else df_new) # ternary operator
        }
    )
