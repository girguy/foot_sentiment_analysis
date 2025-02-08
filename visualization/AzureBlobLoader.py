import json
from utils.azure_blob_utils import (
    create_blob_client_with_connection_string,
    read_blob_from_container
)


class AzureBlobLoader:
    def __init__(self, CONFIG, CONN_STRING_AZURE_STORAGE):
        self.connection_string = CONN_STRING_AZURE_STORAGE
        self.config = CONFIG
        self.blob_service_client = self._create_blob_client()

    def _load_config(self, config_path):
        with open(config_path, 'r') as file:
            return json.load(file)

    def _create_blob_client(self):
        return create_blob_client_with_connection_string(self.connection_string)

    def load_dataframes(self):
        gold_container_name = self.config['gold_container_name']
        folder_name = self.config['folder_name']

        return {
            "df_date": read_blob_from_container(
                gold_container_name, f"{folder_name}/dim_date.parquet", self.blob_service_client, True
                ),
            "df_article": read_blob_from_container(
                gold_container_name, f"{folder_name}/article.parquet", self.blob_service_client, True
                ),
            "df_team": read_blob_from_container(
                gold_container_name, f"{folder_name}/dim_team.parquet", self.blob_service_client, True
                ),
            "df_reaction": read_blob_from_container(
                gold_container_name, f"{folder_name}/reaction.parquet", self.blob_service_client, True
                ),
            "df_sentiment": read_blob_from_container(
                gold_container_name, f"{folder_name}/dim_sentiment.parquet", self.blob_service_client, True
                ),
            "df_fact_reaction": read_blob_from_container(
                gold_container_name, f"{folder_name}/fact_reaction.parquet", self.blob_service_client, True
                ),
            "df_fact_title": read_blob_from_container(
                gold_container_name, f"{folder_name}/fact_title.parquet", self.blob_service_client, True
                ),
        }
