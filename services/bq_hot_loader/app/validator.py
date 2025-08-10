import json
import os
from jsonschema import validate, RefResolver

from app import config

class SchemaValidator:
    def __init__(self):
        self.schema_cache = {}
        self.resolver = RefResolver(
            base_uri=f"file://{config.SCHEMA_DIR}/",
            referrer=None
        )

    def load_schema(self, event_type):
        """Loads schema from disk."""
        if event_type in self.schema_cache:
            return self.schema_cache[event_type]

        # Event type maps to file (lowercase, hyphenated if needed)
        schema_file = f"{event_type.lower().replace('_','-')}.schema.json"
        schema_path = os.path.join(config.SCHEMA_DIR, "events", schema_file)
        with open(schema_path, "r") as f:
            schema = json.load(f)
            self.schema_cache[event_type] = schema
            return schema

    def validate_event(self, event):
        schema = self.load_schema(event["event_type"])
        validate(instance=event, schema=schema, resolver=self.resolver)
