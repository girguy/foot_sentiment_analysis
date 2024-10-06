# Standard library imports
import os
import re
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
from assets.silver_assets.process_raw_epl_news import silver_process_raw_epl_news


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


def keep_pro_reactions(df):
    """
    Filters out professional reactions by removing rows where the 'title' column contains
    unwanted patterns such as 'Did you know?', 'the fans' verdict', and 'Gossip'.
    
    :param df: A Polars DataFrame with a 'title' column.
    :return: A filtered DataFrame without the unwanted rows.
    """

    # Define regex patterns to match unwanted strings in the 'title' column
    patterns_to_filter = [
        r"Did you know?",          # Unwanted phrase
        r"the fans' verdict",       # Unwanted fan-related phrase
        r"Gossip"                  # Unwanted gossip-related articles
    ]

    # Combine the patterns into a single regex expression using the OR operator (|)
    combined_pattern = "|".join(patterns_to_filter)

    # Filter out rows where the 'title' column contains any of the unwanted patterns
    df_filtered = df.filter(~pl.col("title").str.contains(combined_pattern))

    # Return the filtered DataFrame
    return df_filtered

def get_pro_reaction_table(df):
    """
    Processes the DataFrame to generate a table of professional reactions. It first filters out
    unwanted content using the 'keep_pro_reactions' function, renames columns, and adds additional
    columns like 'reaction_id' and 'is_fan'.

    :param df: A Polars DataFrame containing reactions.
    :return: A processed DataFrame with professional reactions and relevant columns.
    """

    # Filter out unwanted professional reactions
    df_processed = keep_pro_reactions(df)

    # Rename columns to have appropriate identifiers
    df_processed = df_processed \
        .rename({"id": "fk_article_id"}) \
        .rename({"publishedDate": "published_at"}) \
        .rename({"teamName": "team_name"})

    # Add 'reaction_id' column by appending '_pro' to 'fk_article_id'
    df_processed = df_processed.with_columns(
        reaction_id=pl.col("fk_article_id") + '_pro'
    )

    df_team = process_team_table(df)

    df_processed = df_processed.join(df_team, on="team_name") \
        .rename({"team_id": "fk_team_id"}) \

    # Add 'is_fan' column to mark these reactions as professional (False)
    df_processed = df_processed.with_columns(
        is_fan=False
    )

    # Return the selected columns for the professional reactions table
    return df_processed.select(["reaction_id", "fk_article_id", "fk_team_id", "content", "published_at", "is_fan"])

# Define a function to extract fan reactions
def extract_reactions(content, publishedDate, article_id, team_name):
    """
    Extracts fan reactions from article content by identifying fan names and reactions
    using regex patterns. Reactions are captured after the phrase "Here are some of your comments:".

    :param content: The article content to extract reactions from.
    :param publishedDate: The published date of the article.
    :param article_id: The ID of the article.
    :return: A list of tuples containing reactionId, reaction text, publishedDate, and article_id.
    """
    
    reactions = []  # List to store extracted reactions

    # Start extracting after "Here are some of your comments:"
    content_after_comments = content.split("Here are some of your comments:")[-1].strip()

    # Regex pattern to extract fan reactions (assuming "Fan Name: Reaction" format)
    pattern = re.compile(r'(\w+):\s+(.+?)(?=\w+:|$)', re.DOTALL)
    matches = pattern.findall(content_after_comments)
    
    # Loop over the matches and append them to the reactions list
    for idx, (fan_name, reaction) in enumerate(matches, start=1):
        reactionId = f"{article_id}_fan_{idx}"  # Create unique reactionId using articleId and index
        reactions.append((reactionId, reaction.strip(), publishedDate, article_id, team_name))  # Append extracted reaction
    
    # Return the list of extracted reactions
    return reactions

def get_fan_reaction_table(df):
    """
    Generates a table of fan reactions by filtering articles where the 'title' contains
    fan-related content (e.g., "the fans' verdict") and extracting reactions using the 
    'extract_reactions' function.

    :param df: A Polars DataFrame containing articles and reactions.
    :return: A DataFrame containing extracted fan reactions.
    """

    # Define regex pattern to match fan-related articles
    patterns_to_filter = r"the fans' verdict"

    # Filter rows where the 'title' column contains the fan-related pattern
    df_filtered = df.filter(pl.col("title").str.contains(patterns_to_filter))

    # Apply the extraction function to the dataframe
    reaction_list = []
    for row in df_filtered.iter_rows(named=True):
        # Extend the reaction list by extracting reactions from each filtered row
        reaction_list.extend(
            extract_reactions(row['content'], row['publishedDate'], row['id'], row['teamName'])
            )

    # Create a new dataframe for the extracted reactions
    df_processed = pl.DataFrame(
        reaction_list,
        schema=['reaction_id', 'content', 'published_at', 'fk_article_id', 'team_name'], orient="row"
    )

    df_team = process_team_table(df)

    df_processed = df_processed.join(df_team, on="team_name") \
        .rename({"team_id": "fk_team_id"}) \

    # Add 'is_fan' column to indicate these reactions are from fans (True)
    df_processed = df_processed.with_columns(
        is_fan=True
    )

    # Return the selected columns for the fan reactions table
    return df_processed.select(['reaction_id', 'fk_article_id', 'fk_team_id', 'content', 'published_at', 'is_fan'])

def create_reaction_table(df):
    """
    Combines professional and fan reactions into a single DataFrame. The fan reactions
    are extracted using 'get_fan_reaction_table', while the professional reactions are
    processed using 'get_pro_reaction_table'. Both tables are concatenated together.

    :param df: A Polars DataFrame containing articles and reactions.
    :return: A concatenated DataFrame containing both fan and professional reactions.
    """

    # Get fan reactions table
    df_fan = get_fan_reaction_table(df)

    # Get professional reactions table
    df_pro = get_pro_reaction_table(df)

    # Concatenate the fan and professional reaction tables
    return pl.concat([df_fan, df_pro])



@asset(
        deps=[silver_process_raw_epl_news],
        group_name="epl_sentiment_analysis",
        compute_kind="polars"
)
def reaction(context: AssetExecutionContext) -> MaterializeResult:
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

    df_processed = create_reaction_table(df)

    # Define the container and path for the blob storage
    gold_container_name = scrapper_config['gold_container_name']
    folder_name = scrapper_config['folder_name']
    path = f"{folder_name}/reaction.parquet"

    write_blob_to_container(df_processed, gold_container_name, path, blob_service_client)

    print("Operation completed successfully.")

    return MaterializeResult(
        metadata={
            "num_records": len(df_processed) # ternary operator
        }
    )