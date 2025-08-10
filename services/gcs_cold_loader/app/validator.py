import json
import os
from jsonschema import validate, ValidationError
from app import config

class EventValidator:
    def __init__(self):
        self.schemas = {}
        for file_name in os.listdir(config.SCHEMA_DIR):
            if file_name.endswith(".json"):
                with open(os.path.join(config.SCHEMA_DIR, file_name)) as f:
                    schema = json.load(f)
                    event_type = schema["title"].upper()
                    self.schemas[event_type] = schema

    def validate_event(self, event):
        event_type = event.get("event_type", "").upper()
        schema = self.schemas.get(event_type)
        if not schema:
            raise ValueError(f"No schema found for event_type: {event_type}")
        try:
            validate(instance=event, schema=schema)
        except ValidationError as e:
            raise ValueError(f"Schema validation failed: {e.message}")
