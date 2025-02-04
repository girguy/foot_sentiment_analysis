import os
import sys
from AzureBlobLoader import AzureBlobLoader


config_path = os.path.join(sys.path[-1], 'scrapper_config.json')
loader = AzureBlobLoader(config_path)
dataframes = loader.load_dataframes()
print("Data loaded successfully!")
