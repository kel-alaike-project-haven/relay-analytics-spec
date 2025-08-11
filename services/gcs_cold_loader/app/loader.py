# services/gcs_cold_loader/app/loader.py
import io
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple

import fastavro
from google.cloud import storage

from app import config

# ------------------------------ helpers ---------------------------------

def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _strip_schema_suffix(name: str) -> str:
    # delivered.schema.json -> delivered
    return name[:-len(".schema.json")] if name.endswith(".schema.json") else os.path.splitext(name)[0]

def _extract_event_properties(schema: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Return a flat dict of properties from an event schema, whether they are
    defined at the root or inside any allOf branch (common pattern when the
    event extends the envelope).
    """
    props: Dict[str, Dict[str, Any]] = dict(schema.get("properties", {}) or {})
    for part in schema.get("allOf", []) or []:
        props.update(part.get("properties", {}) or {})
    return props

def _merge_contract_props(envelope: Dict[str, Any], event_contract: Dict[str, Any]) -> Dict[str, Dict]:
    env_props = envelope.get("properties", {}) or {}
    evt_props = _extract_event_properties(event_contract)
    merged = dict(env_props)
    merged.update(evt_props)
    return merged

def _is_timestamp_prop(prop_schema: Dict[str, Any], name: str) -> bool:
    t = prop_schema.get("type")
    if isinstance(t, list):
        t = next((x for x in t if x != "null"), None)
    if t == "string" and prop_schema.get("format") == "date-time":
        return True
    # heuristic fallback
    return name.endswith("_ts")

def _to_micros(rfc3339: str) -> int:
    # Accept Z or +00:00, etc.
    dt = datetime.fromisoformat(rfc3339.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000)

def _avro_primitive_for_json_type(prop: Dict[str, Any]) -> str:
    t = prop.get("type")
    if isinstance(t, list):
        t = next((x for x in t if x != "null"), None)
    return {
        "string": "string",
        "integer": "long",
        "number": "double",
        "boolean": "boolean",
    }.get(t, "string")

# ------------------------------ loader ----------------------------------

class GCSAvroLoader:
    """
    Schema-aware AVRO writer for cold storage:
    - Loads envelope + event contracts from SCHEMA_DIR
    - Builds AVRO schema (union with null for every field)
    - Converts RFC3339 timestamps to long micros (logicalType: timestamp-micros)
    - Adds any extra producer fields as nullable strings
    """

    def __init__(self):
        self.client = storage.Client()
        self.bucket = self.client.bucket(config.BUCKET_NAME)

        # Load envelope
        self.schema_dir = config.SCHEMA_DIR
        if not self.schema_dir:
            raise RuntimeError("SCHEMA_DIR not set")
        self.envelope = _load_json(os.path.join(self.schema_dir, "event-envelope.schema.json"))

        # Load event contracts
        self.contracts: Dict[str, Dict[str, Any]] = {}
        events_dir = os.path.join(self.schema_dir, "events")
        for fn in os.listdir(events_dir):
            if not fn.endswith(".schema.json"):
                continue
            path = os.path.join(events_dir, fn)
            try:
                contract = _load_json(path)
            except Exception as e:
                print(f"[GCSAvroLoader] Skipping {fn}: {e}")
                continue
            key = _strip_schema_suffix(fn).upper().replace("-", "_")
            self.contracts[key] = contract

        if not self.contracts:
            raise RuntimeError(f"No event contracts loaded from {events_dir}")

        loaded = ", ".join(sorted(self.contracts.keys()))
        print(f"[GCSAvroLoader] Loaded {len(self.contracts)} event contracts: {loaded}")

    # -------- schema composition (AVRO) --------

    def _build_avro_schema_for_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build an AVRO schema for this event by merging envelope + event-contract properties.
        Every field is nullable (["null", <type>]).
        Timestamps become long with logicalType=timestamp-micros.
        Extra fields not in the contract become nullable strings.
        """
        evt_type = (event.get("event_type") or "").upper()
        contract = self.contracts.get(evt_type)
        if not contract:
            raise ValueError(f"No event contract loaded for event_type={evt_type}")

        all_props = _merge_contract_props(self.envelope, contract)
        fields: List[Dict[str, Any]] = []

        # Contract-defined fields first
        for name, prop in all_props.items():
            if _is_timestamp_prop(prop, name):
                avro_type = {"type": "long", "logicalType": "timestamp-micros"}
            else:
                avro_type = _avro_primitive_for_json_type(prop)
            fields.append({"name": name, "type": ["null", avro_type]})

        # Then any extra fields in the event (additive producer fields)
        contract_keys = set(all_props.keys())
        for name, value in event.items():
            if name in contract_keys:
                continue
            # Heuristic: timestamps by suffix, else infer primitive from python type, else string
            if isinstance(value, str) and name.endswith("_ts"):
                fields.append({"name": name, "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]})
            elif isinstance(value, bool):
                fields.append({"name": name, "type": ["null", "boolean"]})
            elif isinstance(value, int):
                fields.append({"name": name, "type": ["null", "long"]})
            elif isinstance(value, float):
                fields.append({"name": name, "type": ["null", "double"]})
            else:
                fields.append({"name": name, "type": ["null", "string"]})

        return {
            "type": "record",
            "name": f"{evt_type}_Event",
            "fields": fields,
        }

    # -------- event normalization for AVRO --------

    def _normalize_for_avro(self, event: Dict[str, Any], avro_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a row that matches the avro_schema:
        - ensure all fields exist (null if missing)
        - convert RFC3339 timestamp strings -> micros (long) when field logicalType is timestamp-micros
        - coerce simple primitives (int/float/bool/string) when possible
        """
        row: Dict[str, Any] = {}

        # Index fields for quick lookups
        field_defs = {f["name"]: f for f in avro_schema.get("fields", [])}

        for name, fdef in field_defs.items():
            value = event.get(name, None)

            # Find the non-null branch of the union
            avro_union = fdef["type"]
            non_null = next((t for t in avro_union if t != "null"), "string")

            # logicalType?
            if isinstance(non_null, dict) and non_null.get("logicalType") == "timestamp-micros":
                if isinstance(value, str) and value:
                    try:
                        row[name] = _to_micros(value)
                    except Exception:
                        # if it isn't parseable, store null rather than invalid
                        row[name] = None
                else:
                    row[name] = None
            else:
                # Primitive coercions
                if non_null == "long":
                    try:
                        row[name] = int(value) if value is not None else None
                    except Exception:
                        row[name] = None
                elif non_null == "double":
                    try:
                        row[name] = float(value) if value is not None else None
                    except Exception:
                        row[name] = None
                elif non_null == "boolean":
                    row[name] = bool(value) if isinstance(value, bool) else (None if value is None else None)
                else:
                    # string or unknown -> stringify except None
                    row[name] = None if value is None else str(value)

        return row

    # -------- public API --------

    def upload_event(self, event: Dict[str, Any]) -> str:
        evt_type = event.get("event_type")
        if not evt_type:
            raise ValueError("Event missing event_type")

        # Build AVRO schema for this event shape
        avro_schema = self._build_avro_schema_for_event(event)

        # Normalize one row per event
        row = self._normalize_for_avro(event, avro_schema)

        # Write AVRO to memory
        buf = io.BytesIO()
        fastavro.writer(buf, avro_schema, [row])
        buf.seek(0)

        # Path: events/YYYY/MM/DD/HH/<event_type>/<event_id>.avro
        now = datetime.now(timezone.utc)
        event_id = row.get("event_id") or "no-id"
        path = f"events/{now:%Y/%m/%d/%H}/{evt_type}/{event_id}.avro"

        blob = self.bucket.blob(path)
        blob.upload_from_file(buf, content_type="application/avro")

        return path
