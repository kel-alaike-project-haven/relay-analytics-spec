import os

PROJECT_ID = os.environ.get("PROJECT_ID")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID")
BRONZE_DATASET = os.environ.get("BRONZE_DATASET", "relay_bronze")
BRONZE_TABLE = os.environ.get("BRONZE_TABLE", "parcel_events")
SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data_contracts", "schemas"
)
