import os
import sys
from AzureBlobLoader import AzureBlobLoader

if __name__ == "__main__":
    
    config_path = os.path.join(sys.path[-1], 'scrapper_config.json')
    loader = AzureBlobLoader(config_path)
    dataframes = loader.load_dataframes()
    print("Data loaded successfully!")
