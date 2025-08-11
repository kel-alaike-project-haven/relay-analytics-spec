import json
from google.cloud import pubsub_v1
from app.validator import EventValidator
from app.loader import BigQueryLoader

from app import config

class PubSubSubscriber:
    def __init__(self):
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(
            config.PROJECT_ID, config.SUBSCRIPTION_ID
        )
        self.validator = EventValidator()
        self.loader = BigQueryLoader()

    def callback(self, message):
        try:
            event = json.loads(message.data.decode("utf-8"))
            self.validator.validate_event(event)
            self.loader.insert_event(event)
            message.ack()
        except Exception as e:
            print(f"Error processing message: {e}")
            message.nack()

    def listen(self):
        streaming_pull_future = self.subscriber.subscribe(
            self.subscription_path, callback=self.callback
        )
        print(f"Listening on {self.subscription_path}...")
        with self.subscriber:
            try:
                streaming_pull_future.result()
            except KeyboardInterrupt:
                streaming_pull_future.cancel()
