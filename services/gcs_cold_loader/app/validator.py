# services/gcs_cold_loader/app/validator.py
import json
from pathlib import Path
from typing import Dict, Any, Optional

from jsonschema import ValidationError, Draft202012Validator, RefResolver
from app import config

ENVELOPE_URL = "https://relay-spec.example/schemas/event-envelope.schema.json"

def _normalize_event_key(s: str) -> str:
    return (s or "").strip().replace("-", "_").upper()

def _strip_schema_suffix(name: str) -> str:
    # delivered.schema.json -> delivered
    return name[:-len(".schema.json")] if name.endswith(".schema.json") else Path(name).stem

def _extract_event_const(schema: Dict[str, Any]) -> Optional[str]:
    try:
        return schema["properties"]["event_type"]["const"]
    except Exception:
        pass
    for part in schema.get("allOf", []) or []:
        try:
            return part["properties"]["event_type"]["const"]
        except Exception:
            continue
    return None

class EventValidator:
    """
    Loads local JSON Schemas and validates events without network calls.
    Mirrors the hot loader behaviour (envelope mapping + file-based keys).
    """

    def __init__(self) -> None:
        self.schema_dir = Path(config.SCHEMA_DIR)
        if not self.schema_dir.exists():
            raise RuntimeError(f"Schema directory not found: {self.schema_dir}")

        print(f"[EventValidator] SCHEMA_DIR={self.schema_dir}")

        # Envelope into store under its remote $id, so $ref can resolve locally
        envelope_path = self.schema_dir / "event-envelope.schema.json"
        with envelope_path.open("r", encoding="utf-8") as f:
            self.envelope_schema = json.load(f)

        env_props = list((self.envelope_schema.get("properties") or {}).keys())
        print(f"[EventValidator] Loaded envelope: {envelope_path.name} "
              f"($id={self.envelope_schema.get('$id')}) props={len(env_props)} -> {env_props}")

        self.store: Dict[str, Dict[str, Any]] = {ENVELOPE_URL: self.envelope_schema}
        self.resolver = RefResolver(
            base_uri=f"file://{self.schema_dir.as_posix()}/",
            referrer=self.envelope_schema,
            store=self.store,
        )

        # Load events/*.schema.json
        events_dir = self.schema_dir / "events"
        if not events_dir.exists():
            raise RuntimeError(f"Events schema directory not found: {events_dir}")

        self.schemas: Dict[str, Dict[str, Any]] = {}
        print(f"[EventValidator] Loading event contracts from: {events_dir}")

        for p in sorted(events_dir.glob("*.schema.json")):
            try:
                with p.open("r", encoding="utf-8") as f:
                    schema = json.load(f)
            except Exception as e:
                print(f"[EventValidator] Skipping {p.name}: {e}")
                continue

            sid = schema.get("$id")
            if isinstance(sid, str) and sid:
                self.store[sid] = schema

            const = _extract_event_const(schema)
            if const:
                key = _normalize_event_key(const)
                source = "event_type.const"
            else:
                fname = _strip_schema_suffix(p.name)
                key = _normalize_event_key(schema.get("title") or fname)
                source = "title/filename"

            if key in ("EVENT_ENVELOPE", "EVENTENVELOPE"):
                continue

            prop_names = list((schema.get("properties") or {}).keys())
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
