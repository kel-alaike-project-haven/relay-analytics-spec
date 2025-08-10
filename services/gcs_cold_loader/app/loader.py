import io
import fastavro
from google.cloud import storage
from datetime import datetime
from app import config

class GCSAvroLoader:
    def __init__(self):
        self.client = storage.Client()
        self.bucket = self.client.bucket(config.BUCKET_NAME)

    def upload_event(self, event):
        # Convert to AVRO
        schema = {
            "type": "record",
            "name": "Event",
            "fields": [{"name": k, "type": ["null", "string"]} for k in event.keys()]
        }

        bytes_io = io.BytesIO()
        fastavro.writer(bytes_io, schema, [event])
        bytes_io.seek(0)

        # Path: events/YYYY/MM/DD/HH/<uuid>.avro
        now = datetime.utcnow()
        path = f"events/{now:%Y/%m/%d/%H}/{event['event_id']}.avro"

        blob = self.bucket.blob(path)
        blob.upload_from_file(bytes_io, content_type="application/avro")

        return path
