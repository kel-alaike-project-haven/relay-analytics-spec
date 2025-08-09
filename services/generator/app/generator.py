"""
Parcel Lifecycle Generator
--------------------------

This module generates a **strictly ordered list of synthetic parcel events** that
simulate a realistic delivery journey, including normal operational events
and a configurable set of exceptions/delays.

The output is designed to match the JSON schema contracts defined in `data_contracts/`
and is consumed by the streaming generator in `app.py`.

Usage:
    events = generate_parcel_lifecycle(datetime.utcnow(), config_dict)

Key design points:
- All timestamps are **non-decreasing** and RFC 3339 formatted.
- Randomness is controlled with a fixed seed at module import for reproducibility.
- Configurable through `cfg` (min/max delays, exception probabilities, ETA behaviour, etc.).
- Produces events in a canonical order: CREATED → IN_DEPOT → OUT_DEPOT → LOADED → OFD → ETA → DELIVERED.

The sequence of events generated for each parcel:
    1. PARCEL_CREATED
    2. SCAN_IN_DEPOT (+ possible depot exceptions)
    3. SCAN_OUT_DEPOT
    4. LOADED_TO_VAN (+ possible breakdown exception)
    5. OUT_FOR_DELIVERY
    6. ETA_SET
    7. ETA_UPDATED (0–2 times)
    8. DELIVERED (possibly multiple attempts)
"""

import random
from datetime import timedelta, datetime, timezone
from typing import Dict, Any, List

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


def generate_parcel_lifecycle(now_base: datetime, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generate a strictly ordered list of parcel lifecycle events.

    Parameters
    now_base : datetime
        Base "creation" timestamp for the parcel (timezone-aware).
    cfg : dict
        Runtime configuration, containing:
            - schema.version
            - lifecycle timing ranges (min/max minutes between stages)
            - exceptions probabilities
            - ETA behaviour

    Returns
    list of dict
        Ordered list of events. Each event dict contains:
            - Common metadata (schema_version, event_version, event_id, parcel_id, etc.)
            - Event-specific attributes (e.g., depot_id, route_id, ETA fields, etc.)

    Notes
    - All timestamps are adjusted to ensure strict chronological order.
    - Some events may be missing depending on exception probabilities (e.g., no ETA updates).
    - Second delivery attempt is generated if first fails or customer not home.
    """
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
        Attach common metadata to an event and increment the sequence number.
        """
        nonlocal seq
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
        return base

    # -------------------------------------------------------------------
    # 1) PARCEL_CREATED
    # -------------------------------------------------------------------
    t_created = now_base - timedelta(minutes=random.randint(0, 2))
    events.append(env({
        "event_type": "PARCEL_CREATED",
        "event_ts": rfc3339(t_created),
        "merchant_id": merchant_id,
        "origin_address_id": origin_address_id,
        "destination_address_id": destination_address_id,
        "service_tier": tier,
        "created_ts": rfc3339(t_created - timedelta(minutes=2)),
        "promised_window_start": rfc3339(pws),
        "promised_window_end": rfc3339(pwe),
        "weight_grams": weight_grams,
        "volume_cm3": volume_cm3
    }))

    # -------------------------------------------------------------------
    # 2) SCAN_IN_DEPOT (+ possible depot exceptions)
    # -------------------------------------------------------------------
    lc = cfg["lifecycle"]
    t_in = t_created + deltas_minutes(lc["in_depot_min"], lc["in_depot_max"])
    events.append(env({
        "event_type": "SCAN_IN_DEPOT",
        "event_ts": rfc3339(t_in),
        "depot_id": depot_id,
        "scanner_id": f"S-{random.randint(1,99):02d}",
        "area_code": "INBOUND-A",
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
        "depot_id": depot_id,
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
        "route_id": route_id,
        "courier_id": courier_id,
        "planned_stop_seq": random.randint(1, 200)
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
        "route_id": route_id,
        "first_planned_eta_ts": rfc3339(t_ofd + first_eta)
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
        "route_id": route_id,
        "predicted_delivery_ts": rfc3339(last_eta),
        "generated_ts": rfc3339(t_eta0),
        "source": "PLANNER"
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
            "route_id": route_id,
            "predicted_delivery_ts": rfc3339(last_eta),
            "generated_ts": rfc3339(last_gen),
            "source": "RECOMPUTE"
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
        "route_id": route_id,
        "courier_id": courier_id
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
            "route_id": route_id,
            "courier_id": courier_id
        }
        # No failure_reason unless second attempt fails
        if second_outcome == "FAILED":
            delivered_event_2["failure_reason"] = "UNSPECIFIED"  # Or something meaningful
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
