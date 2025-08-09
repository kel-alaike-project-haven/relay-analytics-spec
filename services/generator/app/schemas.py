# services/generator/app/schemas.py
"""
Schema registry and validator.

We validate each event against the **event-type schema only**. Every event-type schema
already includes the base envelope via:
    "allOf": [
      { "$ref": "https://relay-spec.example/schemas/event-envelope.schema.json" },
      { ... type-specific requirements ... }
    ]

This means one validation pass enforces BOTH the envelope and the event payload.
Validating against the envelope separately would fail due to `additionalProperties: false`
on fields that belong to the type-specific schema.
"""

import json
import os
from typing import Dict, Tuple

from jsonschema import RefResolver
from jsonschema.validators import validator_for

# Map event_type -> canonical schema $id in /data_contracts/schemas/events
EVENT_SCHEMA_IDS = {
    "PARCEL_CREATED": "https://relay-spec.example/schemas/events/parcel-created.schema.json",
    "SCAN_IN_DEPOT": "https://relay-spec.example/schemas/events/scan-in-depot.schema.json",
    "SCAN_OUT_DEPOT": "https://relay-spec.example/schemas/events/scan-out-depot.schema.json",
    "LOADED_TO_VAN": "https://relay-spec.example/schemas/events/loaded-to-van.schema.json",
    "OUT_FOR_DELIVERY": "https://relay-spec.example/schemas/events/out-for-delivery.schema.json",
    "ETA_SET": "https://relay-spec.example/schemas/events/eta-set.schema.json",
    "ETA_UPDATED": "https://relay-spec.example/schemas/events/eta-updated.schema.json",
    "DELIVERED": "https://relay-spec.example/schemas/events/delivered.schema.json",
    "EXCEPTION": "https://relay-spec.example/schemas/events/exception.schema.json",
}


class SchemaRegistry:
    """
    Loads the envelope + all event-type schemas into an in-memory store for fast
    ref resolution. Validation picks the correct validator for the target schema's
    declared metaschema (draft-07 or 2020-12) automatically.
    """

    def __init__(self, schema_dir: str):
        self.schema_dir = schema_dir
        self.envelope_schema, self.store = self._load_schemas(schema_dir)
        # A RefResolver with `store` lets $ref resolve by $id without hitting the network.
        self.resolver = RefResolver.from_schema(self.envelope_schema, store=self.store)

    def _load_schemas(self, schema_dir: str) -> Tuple[Dict, Dict]:
        """Load envelope and event schemas into a single `$id` -> schema dict."""
        with open(os.path.join(schema_dir, "event-envelope.schema.json"), "r") as f:
            envelope = json.load(f)

        events_dir = os.path.join(schema_dir, "events")
        store: Dict[str, Dict] = {envelope["$id"]: envelope}

        for fname in sorted(os.listdir(events_dir)):
            if not fname.endswith(".schema.json"):
                continue
            with open(os.path.join(events_dir, fname), "r") as f:
                sch = json.load(f)
            # Each event schema must have a unique $id; we index by it.
            store[sch["$id"]] = sch

        return envelope, store

    def validate(self, evt: Dict) -> None:
        """
        Validate a single event dict against its event-type schema.

        Important:
        - We do NOT validate against the envelope separately. Each event schema already
          includes the envelope via `allOf`, so one validation pass enforces both.
        - We select the appropriate validator class based on the target schema's $schema.
        """
        event_type = evt.get("event_type")
        if event_type not in EVENT_SCHEMA_IDS:
            raise ValueError(f"Unknown event_type '{event_type}'")

        schema_id = EVENT_SCHEMA_IDS[event_type]
        schema = self.store.get(schema_id)
        if schema is None:
            raise RuntimeError(f"Schema not loaded for $id={schema_id}")

        # Pick a validator that matches the schema's declared metaschema.
        Validator = validator_for(schema)
        Validator.check_schema(schema)

        # Validate once against the composed event schema (which `$ref`s the envelope).
        Validator(schema, resolver=self.resolver).validate(evt)
