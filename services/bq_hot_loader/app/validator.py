# services/bq_hot_loader/app/validator.py
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional

from jsonschema import ValidationError, Draft202012Validator, RefResolver
from app import config

ENVELOPE_URL = "https://relay-spec.example/schemas/event-envelope.schema.json"


def _normalize_event_key(s: str) -> str:
    return (s or "").strip().replace("-", "_").upper()


def _extract_event_const(schema: Dict[str, Any]) -> Optional[str]:
    # direct
    try:
        return schema["properties"]["event_type"]["const"]
    except Exception:
        pass
    # or inside an allOf branch
    for part in schema.get("allOf", []):
        try:
            return part["properties"]["event_type"]["const"]
        except Exception:
            continue
    return None


class EventValidator:
    """
    Loads local JSON Schemas and validates events without any network calls.
    Adds verbose logging so you can see exactly what's loaded.
    """

    def __init__(self) -> None:
        self.schema_dir = Path(config.SCHEMA_DIR)
        if not self.schema_dir.exists():
            raise RuntimeError(f"Schema directory not found: {self.schema_dir}")

        print(f"[EventValidator] SCHEMA_DIR={self.schema_dir}")

        # Load the local envelope and register it under the remote URL so $ref hits the store.
        envelope_path = self.schema_dir / "event-envelope.schema.json"
        with envelope_path.open("r", encoding="utf-8") as f:
            self.envelope_schema = json.load(f)

        env_props = list(self.envelope_schema.get("properties", {}).keys())
        print(f"[EventValidator] Loaded envelope: {envelope_path.name} "
              f"($id={self.envelope_schema.get('$id')}) "
              f"props={len(env_props)} -> {env_props}")

        self.store: Dict[str, Dict[str, Any]] = {ENVELOPE_URL: self.envelope_schema}
        self.resolver = RefResolver(
            base_uri=f"file://{self.schema_dir.as_posix()}/",
            referrer=self.envelope_schema,
            store=self.store,
        )

        # Load all event contracts under .../schemas/events/*.schema.json
        events_dir = self.schema_dir / "events"
        if not events_dir.exists():
            raise RuntimeError(f"Events schema directory not found: {events_dir}")

        print(f"[EventValidator] Loading event contracts from: {events_dir}")

        self.schemas: Dict[str, Dict[str, Any]] = {}
        for p in sorted(events_dir.glob("*.schema.json")):
            try:
                with p.open("r", encoding="utf-8") as f:
                    schema = json.load(f)
            except Exception as e:
                print(f"[EventValidator] Skipping {p.name}: {e}")
                continue

            # Register by $id too (helps any $ref by URL)
            sid = schema.get("$id")
            if isinstance(sid, str) and sid:
                self.store[sid] = schema

            const = _extract_event_const(schema)
            if const:
                key = _normalize_event_key(const)
                source = "event_type.const"
            else:
                title = schema.get("title")
                if title:
                    key = _normalize_event_key(title)
                    source = "title"
                else:
                    key = _normalize_event_key(p.stem)
                    source = "filename"

            # Skip envelope if ever found here
            if key in ("EVENT_ENVELOPE", "EVENTENVELOPE"):
                continue

            prop_names = list(schema.get("properties", {}).keys())
            print(f"[EventValidator] Loaded contract: file={p.name} key={key} "
                  f"(from {source}) props={len(prop_names)} -> {prop_names}")

            self.schemas[key] = schema

        if not self.schemas:
            raise RuntimeError(f"No event schemas loaded from {events_dir}")

        loaded_keys = ", ".join(sorted(self.schemas.keys()))
        print(f"[EventValidator] Loaded {len(self.schemas)} contracts: {loaded_keys}")

    def validate_event(self, event: Dict[str, Any]) -> None:
        raw_key = event.get("event_type", "")
        event_key = _normalize_event_key(str(raw_key))
        schema = self.schemas.get(event_key)

        if schema is None:
            loaded = ", ".join(sorted(self.schemas.keys()))
            raise ValueError(
                f"No event contract loaded for event_type={repr(raw_key)} "
                f"(normalized={repr(event_key)}). Loaded={loaded}"
            )

        try:
            Draft202012Validator(schema, resolver=self.resolver).validate(event)
        except ValidationError as e:
            path = ".".join(map(str, e.absolute_path)) or "<root>"
            raise ValueError(f"Schema validation failed at {path}: {e.message}")
