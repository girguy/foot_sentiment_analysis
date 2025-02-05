import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from AzureBlobLoader import AzureBlobLoader
from utils.config import CONFIG, CONN_STRING_AZURE_STORAGE


def load_data():
    try:
        loader = AzureBlobLoader(CONFIG, CONN_STRING_AZURE_STORAGE)
        dataframes = loader.load_dataframes()
        print("Data loaded successfully!")
        return dataframes
    except Exception as e:
        print(f"Error loading data: {e}", exc_info=True)


if __name__ == "__main__":
    dataframes = load_data()
