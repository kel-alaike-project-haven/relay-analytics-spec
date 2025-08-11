"""
Parcel Lifecycle Generator (Schema-Aware)
-----------------------------------------

Generates a strictly ordered list of synthetic parcel events that simulate a
realistic delivery journey. This version is **schema-aware**: it loads your
JSON Schemas (envelope + per-event) and only injects fields that each event
type is allowed to carry. That keeps BigQuery tidy and avoids validation issues.

- All timestamps are non-decreasing and RFC3339 UTC.
- Randomness is seeded for reproducibility.
- Sequencing and event timing logic unchanged from your original.
- Common fields (e.g., trace_id, generated_ts, route_id, depot_id) are injected
  *only if* allowed by the JSON schema for the current event type.

Repo assumptions for schema paths (relative to this file):
  services/
    data_contracts/schemas/
      event-envelope.schema.json
      events/*.schema.json
"""

import os
import json
import random
from datetime import timedelta, datetime, timezone
from typing import Dict, Any, List, Set

from jsonschema import RefResolver

from .utils import uuid4, rfc3339, clamp, deltas_minutes, normal_minutes

# ---------------------------------------------------------------------------
# Static ID pools for deterministic lookups (seeded for reproducibility).
# These act like "reference tables" of depot IDs, courier IDs, merchant IDs.
# ---------------------------------------------------------------------------
random.seed(42)
DEPOTS = [uuid4() for _ in range(10)]
COURIERS = [uuid4() for _ in range(300)]
MERCHANTS = [uuid4() for _ in range(120)]


def _choose(lst):
    """Pick a random element from a list."""
    return random.choice(lst)


def _service_tier() -> str:
    """
    Assign a random service tier based on weighted probability.
    60% → NEXT_DAY, 30% → TWO_DAY, 10% → ECONOMY.
    """
    r = random.random()
    if r < 0.6:
        return "NEXT_DAY"
    if r < 0.9:
        return "TWO_DAY"
    return "ECONOMY"


# ---------------------------------------------------------------------------
# Schema-aware helpers
# ---------------------------------------------------------------------------

# Cache for allowed fields per event_type
_ALLOWED_FIELDS_CACHE: Dict[str, Set[str]] = {}
# Cache for envelope properties
_ENVELOPE_PROPS: Set[str] = set()
# Absolute schema dir cache flag
_SCHEMAS_LOADED = False


def _schema_dir_default() -> str:
    """
    Resolve the default schema directory:
      current file: services/generator/app/generator.py
      schema dir :  services/data_contracts/schemas
    """
    here = os.path.dirname(os.path.abspath(__file__))
    services_root = os.path.abspath(os.path.join(here, "..", ".."))
    return os.path.join(services_root, "data_contracts", "schemas")


def _load_schemas_once(schema_dir: str) -> None:
    """
    Load the envelope and all event schemas once and build the allowed field sets.
    """
    global _SCHEMAS_LOADED, _ENVELOPE_PROPS, _ALLOWED_FIELDS_CACHE
    if _SCHEMAS_LOADED:
        return

    envelope_path = os.path.join(schema_dir, "event-envelope.schema.json")
    events_dir = os.path.join(schema_dir, "events")

    if not os.path.exists(envelope_path):
        raise FileNotFoundError(f"Envelope schema not found: {envelope_path}")
    if not os.path.isdir(events_dir):
        raise FileNotFoundError(f"Events schema dir not found: {events_dir}")

    with open(envelope_path, "r") as f:
        envelope_schema = json.load(f)

    _ENVELOPE_PROPS = set(envelope_schema.get("properties", {}).keys())

    # Build a local resolver so $ref to the envelope URL resolves to this file (no network)
    ENVELOPE_URL = "https://relay-spec.example/schemas/event-envelope.schema.json"
    resolver = RefResolver(
        base_uri=f"file://{schema_dir}/",
        referrer=None,
        store={ENVELOPE_URL: envelope_schema},
    )

    # For each event schema, merge envelope properties + event-specific properties
    for fname in os.listdir(events_dir):
        if not fname.endswith(".schema.json"):
            continue
        path = os.path.join(events_dir, fname)
        with open(path, "r") as f:
            schema = json.load(f)

        # Derive event_type from filename: 'parcel-created.schema.json' -> 'PARCEL_CREATED'
        stem = fname.replace(".schema.json", "")
        event_type = stem.upper().replace("-", "_")

        allowed: Set[str] = set(_ENVELOPE_PROPS)

        # The second object in allOf is the event-specific properties block
        if "allOf" in schema and len(schema["allOf"]) >= 2:
            specific = schema["allOf"][1]
            for k in specific.get("properties", {}).keys():
                allowed.add(k)

        _ALLOWED_FIELDS_CACHE[event_type] = allowed

    _SCHEMAS_LOADED = True


def _allowed_fields_for(event_type: str, schema_dir: str) -> Set[str]:
    """
    Get the allowed field set for this event type. Loads schemas on first call.
    """
    _load_schemas_once(schema_dir)
    return _ALLOWED_FIELDS_CACHE.get(event_type, set(_ENVELOPE_PROPS))


def _inject_common_fields(evt: Dict[str, Any],
                          allowed: Set[str],
                          context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Given an event payload, the set of allowed fields for its type, and a context
    dict containing candidate common values (route_id, depot_id, etc.), copy in
    fields **only if** allowed by schema and not already present in evt.
    """
    for k, v in context.items():
        if k in allowed and (k not in evt or evt[k] is None):
            evt[k] = v
    return evt


# ---------------------------------------------------------------------------
# Main lifecycle generator
# ---------------------------------------------------------------------------

def generate_parcel_lifecycle(now_base: datetime,
                              cfg: Dict[str, Any],
                              schema_dir: str = None) -> List[Dict[str, Any]]:
    """
    Generate a strictly ordered list of parcel lifecycle events.

    Args
    ----
    now_base : datetime (tz-aware)
        Base "creation" timestamp for the parcel.
    cfg : dict
        Runtime configuration, containing:
          - schema.version
          - lifecycle timing ranges (min/max minutes between stages)
          - exceptions probabilities
          - ETA behaviour
    schema_dir : str, optional
        Absolute path to the directory containing:
          event-envelope.schema.json and events/*.schema.json
        If None, a sensible default relative to this file is used.

    Returns
    -------
    List[Dict[str, Any]] : ordered list of event payloads.
    """
    # Resolve schema_dir default lazily
    if schema_dir is None:
        schema_dir = _schema_dir_default()

    # Common metadata for all events
    schema_version = cfg["schema"]["version"]
    event_version = cfg["schema"]["version"]
    producer = "generator"

    # Generate fixed IDs for this parcel lifecycle
    parcel_id = uuid4()
    merchant_id = _choose(MERCHANTS)
    origin_address_id = uuid4()
    destination_address_id = uuid4()
    depot_id = _choose(DEPOTS)
    courier_id = _choose(COURIERS)
    route_id = uuid4()

    # Determine promised delivery window based on service tier
    tier = _service_tier()
    if tier == "NEXT_DAY":
        pws = now_base + timedelta(hours=8)
        pwe = now_base + timedelta(hours=20)
    elif tier == "TWO_DAY":
        pws = now_base + timedelta(hours=32)
        pwe = now_base + timedelta(hours=44)
    else:  # ECONOMY
        pws = now_base + timedelta(hours=56)
        pwe = now_base + timedelta(hours=68)

    # Random weight & volume within plausible operational bounds
    weight_grams = int(clamp(random.lognormvariate(6.7, 0.4), 0, 20000))
    volume_cm3 = int(clamp(random.lognormvariate(7.1, 0.5), 0, 80000))

    # Sequence counter ensures unique ordering within a parcel's events
    seq = 0
    events: List[Dict[str, Any]] = []

    def env(evt: Dict[str, Any]) -> Dict[str, Any]:
        """
        Attach envelope metadata, inject schema-allowed common fields, increment sequence.
        """
        nonlocal seq
        event_type = evt.get("event_type", "UNKNOWN")

        # Envelope (always allowed)
        base = {
            "schema_version": schema_version,
            "event_version": event_version,
            "event_id": uuid4(),
            "parcel_id": parcel_id,
            "producer": producer,
            "sequence_no": seq
        }
        seq += 1
        base.update(evt)

        # Determine which fields are allowed for this event_type
        allowed = _allowed_fields_for(event_type, schema_dir)

        # Build candidate common context (ONLY injected if allowed by event schema)
        common_context = {
            "trace_id": uuid4(),
            "generated_ts": rfc3339(datetime.now(timezone.utc)),
            "route_id": route_id,
            "depot_id": depot_id,
            "weight_grams": weight_grams,       # Allowed on PARCEL_CREATED
            "volume_cm3": volume_cm3,          # Allowed on PARCEL_CREATED
            "area_code": f"AREA-{random.randint(100, 999)}",
            "belt_no": random.randint(1, 20),
            "stage_hint": random.choice(["FIRST_MILE", "DEPOT", "LINEHAUL", "LAST_MILE"]),
            "merchant_id": merchant_id,        # Allowed on PARCEL_CREATED
            "origin_address_id": origin_address_id,       # Allowed on PARCEL_CREATED
            "destination_address_id": destination_address_id,  # Allowed on PARCEL_CREATED
        }

        # Inject only allowed fields that are not already set
        enriched = _inject_common_fields(base, allowed, common_context)
        return enriched

    # -------------------------------------------------------------------
    # 1) PARCEL_CREATED
    # -------------------------------------------------------------------
    t_created = now_base - timedelta(minutes=random.randint(0, 2))
    events.append(env({
        "event_type": "PARCEL_CREATED",
        "event_ts": rfc3339(t_created),
        "service_tier": tier,
        "created_ts": rfc3339(t_created - timedelta(minutes=2)),
        "promised_window_start": rfc3339(pws),
        "promised_window_end": rfc3339(pwe),
        # merchant_id / addresses / weight / volume are injected by env()
    }))

    # -------------------------------------------------------------------
    # 2) SCAN_IN_DEPOT (+ possible depot exceptions)
    # -------------------------------------------------------------------
    lc = cfg["lifecycle"]
    t_in = t_created + deltas_minutes(lc["in_depot_min"], lc["in_depot_max"])
    events.append(env({
        "event_type": "SCAN_IN_DEPOT",
        "event_ts": rfc3339(t_in),
        "scanner_id": f"S-{random.randint(1,99):02d}",
        "area_code": "INBOUND-A",  # explicit (also would be injected if allowed)
        "belt_no": random.randint(1, 5)
    }))

    add_delay = timedelta(0)
    ex = cfg["exceptions"]

    # Missort exception
    if random.random() < ex["MISSORT"]:
        events.append(env({
            "event_type": "EXCEPTION",
            "event_ts": rfc3339(t_in + timedelta(seconds=1)),
            "exception_code": "MISSORT",
            "stage_hint": "DEPOT",
            "details": "Parcel routed to incorrect belt"
        }))
        add_delay += timedelta(minutes=random.randint(30, 90))

    # Depot capacity exception
    if random.random() < ex["DEPOT_CAPACITY"]:
        events.append(env({
            "event_type": "EXCEPTION",
            "event_ts": rfc3339(t_in + timedelta(seconds=2)),
            "exception_code": "DEPOT_CAPACITY",
            "stage_hint": "DEPOT",
            "details": "Temporary capacity constraint"
        }))
        add_delay += timedelta(minutes=random.randint(60, 180))

    # -------------------------------------------------------------------
    # 3) SCAN_OUT_DEPOT
    # -------------------------------------------------------------------
    t_out = t_in + deltas_minutes(lc["out_depot_min"], lc["out_depot_max"]) + add_delay
    events.append(env({
        "event_type": "SCAN_OUT_DEPOT",
        "event_ts": rfc3339(t_out),
        "scanner_id": f"S-{random.randint(1,99):02d}",
        "area_code": "OUTBOUND-B",
        "belt_no": random.randint(1, 5)
    }))

    # -------------------------------------------------------------------
    # 4) LOADED_TO_VAN (+ possible breakdown)
    # -------------------------------------------------------------------
    t_loaded = t_out + deltas_minutes(lc["loaded_min"], lc["loaded_max"])
    events.append(env({
        "event_type": "LOADED_TO_VAN",
        "event_ts": rfc3339(t_loaded),
        "courier_id": courier_id,
        "planned_stop_seq": random.randint(1, 200)
        # route_id injected by env() if allowed
    }))

    breakdown_delay = timedelta(0)
    if random.random() < ex["VEHICLE_BREAKDOWN"]:
        t_break = t_loaded + timedelta(minutes=random.randint(1, 10))
        events.append(env({
            "event_type": "EXCEPTION",
            "event_ts": rfc3339(t_break),
            "exception_code": "VEHICLE_BREAKDOWN",
            "stage_hint": "LAST_MILE",
            "details": "Temporary breakdown, route delayed"
        }))
        breakdown_delay += timedelta(minutes=random.randint(60, 120))

    # -------------------------------------------------------------------
    # 5) OUT_FOR_DELIVERY
    # -------------------------------------------------------------------
    t_ofd = t_loaded + deltas_minutes(lc["ofd_min"], lc["ofd_max"]) + breakdown_delay
    first_eta = normal_minutes(cfg["eta"]["mean_minutes"], cfg["eta"]["sd_minutes"], 15)
    events.append(env({
        "event_type": "OUT_FOR_DELIVERY",
        "event_ts": rfc3339(t_ofd),
        "first_planned_eta_ts": rfc3339(t_ofd + first_eta)
        # route_id injected by env() if allowed
    }))

    # -------------------------------------------------------------------
    # 6) ETA_SET
    # -------------------------------------------------------------------
    t_eta0 = t_ofd + timedelta(minutes=random.randint(0, 2))
    travel = normal_minutes(cfg["eta"]["mean_minutes"], cfg["eta"]["sd_minutes"], 15)
    last_eta = t_ofd + travel
    last_gen = t_eta0
    events.append(env({
        "event_type": "ETA_SET",
        "event_ts": rfc3339(t_eta0),
        "predicted_delivery_ts": rfc3339(last_eta),
        "generated_ts": rfc3339(t_eta0),  # explicit; env() also sets if allowed
        "source": "PLANNER"
        # route_id injected by env() if allowed
    }))

    # -------------------------------------------------------------------
    # 7) ETA_UPDATED (0–2 times)
    # -------------------------------------------------------------------
    updates = 0
    if random.random() < cfg["eta"]["update_prob"]:
        updates = 1 if random.random() < 0.7 else 2
    for _ in range(updates):
        last_gen = last_gen + timedelta(minutes=random.randint(5, 30))
        last_eta = last_eta + timedelta(minutes=random.randint(-15, 25))
        events.append(env({
            "event_type": "ETA_UPDATED",
            "event_ts": rfc3339(last_gen),
            "predicted_delivery_ts": rfc3339(last_eta),
            "generated_ts": rfc3339(last_gen),
            "source": "RECOMPUTE"
            # route_id injected by env() if allowed
        }))

    # -------------------------------------------------------------------
    # 8) DELIVERED (+ second attempt if failed/carded)
    # -------------------------------------------------------------------
    outcome = "SUCCESS"
    failure_reason = None
    attempt_number = 1
    addr_issue = random.random() < ex["ADDRESS_ISSUE"]
    not_home = (not addr_issue) and (random.random() < ex["CUSTOMER_NOT_HOME"])

    noise = timedelta(minutes=random.gauss(0, 15))
    t_delivered_first = t_ofd + travel + noise
    if t_delivered_first < t_ofd + timedelta(minutes=10):
        t_delivered_first = t_ofd + timedelta(minutes=10)

    if addr_issue:
        outcome = "FAILED"
        failure_reason = "ADDRESS_ISSUE"
    elif not_home:
        outcome = "CARDED"

    delivered_event = {
        "event_type": "DELIVERED",
        "event_ts": rfc3339(t_delivered_first),
        "delivered_ts": rfc3339(t_delivered_first),
        "attempt_number": attempt_number,
        "outcome": outcome,
        "courier_id": courier_id
        # route_id injected by env() if allowed
    }
    if failure_reason:  # Only include if not None/empty
        delivered_event["failure_reason"] = failure_reason
    events.append(env(delivered_event))

    # Second attempt logic
    if outcome in ("CARDED", "FAILED"):
        attempt_number = 2
        t_second = t_delivered_first + timedelta(hours=random.randint(4, 28))
        second_outcome = "SUCCESS" if random.random() < 0.85 else (
            "FAILED" if random.random() < 0.6 else "RETURNED"
        )
        delivered_event_2 = {
            "event_type": "DELIVERED",
            "event_ts": rfc3339(t_second),
            "delivered_ts": rfc3339(t_second),
            "attempt_number": attempt_number,
            "outcome": second_outcome,
            "courier_id": courier_id
        }
        if second_outcome == "FAILED":
            delivered_event_2["failure_reason"] = "UNSPECIFIED"
        events.append(env(delivered_event_2))

    # -------------------------------------------------------------------
    # Final pass: ensure event_ts are strictly non-decreasing
    # -------------------------------------------------------------------
    prev = datetime.min.replace(tzinfo=timezone.utc)
    for e in events:
        ts = datetime.fromisoformat(e["event_ts"].replace("Z", "+00:00"))
        if ts < prev:
            ts = prev + timedelta(seconds=1)
            e["event_ts"] = rfc3339(ts)
        prev = ts

    return events
