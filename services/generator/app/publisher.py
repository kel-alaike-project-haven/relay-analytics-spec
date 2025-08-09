# app/publisher.py
import json
from typing import Dict
from google.cloud import pubsub_v1

class PubSubPublisher:
    def __init__(self, project_id: str, topic: str):
        # ✅ Enable message ordering on the client
        self.client = pubsub_v1.PublisherClient(
            publisher_options=pubsub_v1.types.PublisherOptions(
                enable_message_ordering=True
            )
        )
        self.topic_path = self.client.topic_path(project_id, topic)

    def publish(self, evt: Dict) -> None:
        data = json.dumps(evt).encode("utf-8")
        # Use parcel_id as ordering key
        ordering_key = evt["parcel_id"]
        # Optional useful attributes for filtering/observability
        attrs = {
            "event_type": evt.get("event_type", ""),
            "schema_version": evt.get("schema_version", ""),
            "event_version": evt.get("event_version", ""),
        }
        future = self.client.publish(
            self.topic_path,
            data=data,
            ordering_key=ordering_key,
            **attrs
        )
        future.result(timeout=10)  # surface publish errors promptly in dev
