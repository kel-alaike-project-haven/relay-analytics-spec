import os

PROJECT_ID = os.environ.get("PROJECT_ID", "relay-analytics-demo")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID", "sub-cold-gcs")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "relay-cold-storage")
SCHEMA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data_contracts", "schemas"
)
