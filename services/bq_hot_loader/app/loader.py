# services/bq_hot_loader/app/loader.py
import json
import os
import time
from typing import Dict, List, Tuple

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict, PreconditionFailed

from app import config

# Schema helpers: load JSON Schemas and map to BigQuery types
_JSON_TO_BQ_PRIMITIVES = {
    "string": "STRING",
    "integer": "INT64",
    "number": "NUMERIC",
    "boolean": "BOOL",
}

def _load_json(path: str) -> Dict:
    with open(path, "r") as f:
        return json.load(f)

def _bq_type_from_json_schema(field_name: str, field_schema: Dict) -> str:
    t = field_schema.get("type")
    if isinstance(t, list):
        t = next((x for x in t if x != "null"), None)

    if t == "string":
        if field_schema.get("format") == "date-time":
            return "TIMESTAMP"
        return "STRING"
    if t in ("integer", "number", "boolean"):
        return _JSON_TO_BQ_PRIMITIVES.get(t, "STRING")

    if isinstance(field_name, str):
        if field_name.endswith("_ts"):
            return "TIMESTAMP"
        if field_name.endswith("_id"):
            return "STRING"

    return "STRING"

def _merge_contract_props(envelope: Dict, event_contract: Dict) -> Dict[str, Dict]:
    """
    Merge envelope properties with the event's own properties
    (supports schemas that put their props inside allOf).
    """
    env_props = envelope.get("properties", {}) or {}
    evt_props = _extract_event_properties(event_contract)
    merged = dict(env_props)
    merged.update(evt_props)
    return merged

def _fill_missing_fields(event: Dict, all_props: Dict[str, Dict]) -> Dict:
    filled = dict(event)
    for k in all_props.keys():
        if k not in filled:
            filled[k] = None
    return filled

def _bq_schema_from_contract_props(all_props: Dict[str, Dict]) -> List[bigquery.SchemaField]:
    fields: List[bigquery.SchemaField] = []
    for name, prop in all_props.items():
        bq_type = _bq_type_from_json_schema(name, prop)
        fields.append(bigquery.SchemaField(name, bq_type, mode="NULLABLE"))
    return fields

def _append_new_fields(existing: List[bigquery.SchemaField],
                       desired: List[bigquery.SchemaField]) -> Tuple[List[bigquery.SchemaField], bool]:
    by_name = {f.name: f for f in existing}
    changed = False
    for f in desired:
        if f.name not in by_name:
            by_name[f.name] = bigquery.SchemaField(f.name, f.field_type, mode="NULLABLE")
            changed = True
    return list(by_name.values()), changed

def _extract_event_properties(schema: Dict) -> Dict[str, Dict]:
    """
    Return a flat dict of properties from an event schema, whether they are
    defined at the root or inside any allOf branch (common pattern when the
    event extends the envelope).
    """
    props: Dict[str, Dict] = dict(schema.get("properties", {}))
    for part in schema.get("allOf", []):
        props.update(part.get("properties", {}) or {})
    return props

# Loader

class BigQueryLoader:
    """
    Schema-aware BigQuery loader for append-only event streams with verbose visibility.
    """

    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = config.PROJECT_ID
        self.dataset_id = getattr(config, "BRONZE_DATASET", "relay_bronze")
        self.table_name = getattr(config, "BRONZE_TABLE", "parcel_events")
        self.table_id = f"{self.project_id}.{self.dataset_id}.{self.table_name}"

        self.schema_dir = getattr(config, "SCHEMA_DIR")
        if not self.schema_dir:
            raise RuntimeError("SCHEMA_DIR must be set in config for schema-aware loader.")

        # Envelope
        self.envelope = _load_json(os.path.join(self.schema_dir, "event-envelope.schema.json"))
        env_props = list((self.envelope.get("properties") or {}).keys())
        print(f"[BigQueryLoader] Loaded envelope: event-envelope.schema.json props={len(env_props)} -> {env_props}")

        # Event contracts (NO '.SCHEMA' suffix; keys match event_type)
        self.event_contracts: Dict[str, Dict] = {}
        events_dir = os.path.join(self.schema_dir, "events")
        print(f"[BigQueryLoader] Loading event contracts from: {events_dir}")

        for fn in os.listdir(events_dir):
            if not fn.endswith(".schema.json"):
                continue
            path = os.path.join(events_dir, fn)
            try:
                contract = _load_json(path)
            except Exception as e:
                print(f"[BigQueryLoader]   ! skip {fn}: {e}")
                continue

            basename = fn
            if basename.endswith(".schema.json"):
                basename = basename[:-len(".schema.json")]  # strip both extensions
            key = basename.upper().replace("-", "_")       # e.g. PARCEL_CREATED
            self.event_contracts[key] = contract

            props = list(_extract_event_properties(contract).keys())
            print(f"[BigQueryLoader]   -> {fn}: key={key} props={len(props)} {props}")

        if not self.event_contracts:
            raise RuntimeError(f"No event contracts loaded from {events_dir}")

        loaded_keys = ", ".join(sorted(self.event_contracts.keys()))
        print(f"[BigQueryLoader] Loaded {len(self.event_contracts)} contracts: {loaded_keys}")

        self.table_ready = False

    def _contract_for_event(self, event_type: str) -> Dict:
        key = (event_type or "").upper()
        contract = self.event_contracts.get(key)
        if not contract:
            loaded = ", ".join(sorted(self.event_contracts.keys()))
            raise ValueError(
                f"No event contract loaded for event_type={event_type} (lookup key={key}). "
                f"Loaded={loaded}"
            )
        return contract

    def _desired_schema_fields(self, event: Dict) -> List[bigquery.SchemaField]:
        evt_type = event.get("event_type", "")
        contract = self._contract_for_event(evt_type)
        all_props = _merge_contract_props(self.envelope, contract)

        # Log merged contract keys used for BQ schema
        print(f"[BigQueryLoader] Building BQ schema for {evt_type}: "
              f"{len(all_props)} contract fields -> {list(all_props.keys())}")

        desired = _bq_schema_from_contract_props(all_props)

        # Include extra producer fields not in the contract
        contract_keys = set(all_props.keys())
        extras = [k for k in event.keys() if k not in contract_keys]
        if extras:
            print(f"[BigQueryLoader]   Extra fields in event (added additively): {extras}")
        for k in extras:
            v = event[k]
            if isinstance(v, bool):
                t = "BOOL"
            elif isinstance(v, int):
                t = "INT64"
            elif isinstance(v, float):
                t = "NUMERIC"
            else:
                t = "TIMESTAMP" if isinstance(v, str) and k.endswith("_ts") else "STRING"
            desired.append(bigquery.SchemaField(k, t, mode="NULLABLE"))

        # Show a preview of the final BQ schema (name:type)
        preview = [f"{f.name}:{f.field_type}" for f in desired]
        print(f"[BigQueryLoader] Final desired BQ schema ({len(desired)} fields): {preview}")

        return desired

    def _ensure_table_once(self, first_event: Dict):
        try:
            self.client.get_table(self.table_id)
            self.table_ready = True
            print(f"[BigQueryLoader] Found existing table: {self.table_id}")
            return
        except NotFound:
            pass

        desired_fields = self._desired_schema_fields(first_event)

        table = bigquery.Table(self.table_id, schema=desired_fields)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="event_ts",
        )
        table.require_partition_filter = True
        table.clustering_fields = ["parcel_id", "event_type"]

        try:
            self.client.create_table(table)
            print(f"[BigQueryLoader] Created BigQuery table {self.table_id}")
        except Conflict:
            print(f"[BigQueryLoader] Table {self.table_id} already exists after conflict — continuing.")

        self.table_ready = True

    def _ensure_schema_superset(self, event: Dict, max_retries: int = 3):
        desired_fields = self._desired_schema_fields(event)

        for attempt in range(max_retries):
            table = self.client.get_table(self.table_id)
            merged, changed = _append_new_fields(table.schema, desired_fields)
            if not changed:
                return

            table.schema = merged
            try:
                self.client.update_table(table, ["schema"])
                print(f"[BigQueryLoader] Extended schema for {self.table_id} with new columns.")
                return
            except PreconditionFailed:
                if attempt < max_retries - 1:
                    backoff = 2 ** attempt
                    print(f"[BigQueryLoader] Schema update race; retrying in {backoff}s...")
                    time.sleep(backoff)
                else:
                    print(f"[BigQueryLoader] Failed to update schema after {max_retries} attempts.")
                    return

    def insert_event(self, event: Dict):
        # Ensure table created
        self._ensure_table_once(event)

        # Fill missing contract fields with None (so all contract cols are present)
        contract = self._contract_for_event(event.get("event_type", ""))
        all_props = _merge_contract_props(self.envelope, contract)
        event_filled = _fill_missing_fields(event, all_props)

        # Ensure schema can accept all keys
        self._ensure_schema_superset(event_filled)

        # Insert row
        errors = self.client.insert_rows_json(
            self.table_id,
            [event_filled],
            ignore_unknown_values=True,
        )
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
