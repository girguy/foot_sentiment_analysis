import os
from dotenv import load_dotenv


def load_env_variables():
    load_dotenv()
    connection_string = os.getenv("CONN_STRING_AZURE_STORAGE")
    if connection_string is None:
        raise EnvironmentError("Azure storage connection string not found in environment variables.")
    return connection_string


CONFIG = {
    "base_url": "https://www.bbc.com/sport/football/teams",
    "nb_page": 10,
    "bronze_container_name": "bronze",
    "silver_container_name": "silver",
    "gold_container_name": "gold",
    "folder_name": "epl_news",
    "silver_blob_name": "processed_data",
    "connection_string": "CONN_STRING_AZURE_STORAGE",
    "teams":
        {
            "AFC Bournemouth": "afc-bournemouth",
            "Arsenal": "arsenal",
            "Aston Villa": "aston-villa",
            "Brentford": "brentford",
            "Brighton & Hove Albion": "brighton-and-hove-albion",
            "Chelsea": "chelsea",
            "Crystal Palace": "crystal-palace",
            "Everton": "everton",
            "Fulham": "fulham",
            "Ipswich Town": "ipswich-town",
            "Leicester City": "leicester-city",
            "Liverpool": "liverpool",
            "Manchester City": "manchester-city",
            "Manchester United": "manchester-united",
            "Newcastle United": "newcastle-united",
            "Nottingham Forest": "nottingham-forest",
            "Southampton": "southampton",
            "Tottenham Hotspur": "tottenham-hotspur",
            "West Ham United": "west-ham-united",
            "Wolverhampton Wanderers": "wolverhampton-wanderers"
        }
}

CONN_STRING_AZURE_STORAGE = load_env_variables()
