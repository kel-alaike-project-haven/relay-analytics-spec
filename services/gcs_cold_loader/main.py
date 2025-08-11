import base64
import json
import binascii
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from app.validator import EventValidator
from app.loader import GCSAvroLoader
from app import config

def callback(message):
    try:
        raw_bytes = message.data
        try:
            # Attempt base64 decode with validation
            decoded_bytes = base64.b64decode(raw_bytes, validate=True)
            json_str = decoded_bytes.decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            # If not base64, assume it's already UTF-8 JSON
            json_str = raw_bytes.decode("utf-8")

        data = json.loads(json_str)

        validator.validate_event(data)
        path = loader.upload_event(data)
        print(f"Uploaded to {path}")

        message.ack()

    except Exception as e:
        print(f"Error processing message: {e}")
        message.nack()


if __name__ == "__main__":
    validator = EventValidator()
    loader = GCSAvroLoader()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(config.PROJECT_ID, config.SUBSCRIPTION_ID)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
