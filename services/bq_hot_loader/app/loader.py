from google.cloud import bigquery
from app import config

class BigQueryLoader:
    def __init__(self):
        self.client = bigquery.Client()

    def insert_event(self, event):
        table_id = f"{config.PROJECT_ID}.{config.BRONZE_DATASET}.{config.BRONZE_TABLE}"
        errors = self.client.insert_rows_json(table_id, [event])
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
