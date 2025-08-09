"""
Relay Generator API
-------------------

FastAPI application that streams **synthetic parcel event lifecycles** to a Pub/Sub
topic at a roughly Poisson rate. Each lifecycle produces a sequence of contract-first
events (e.g., PARCEL_CREATED → SCAN_IN_DEPOT → … → DELIVERED), validated against
JSON Schemas before publishing. Events are published with `ordering_key = parcel_id`
to preserve per-parcel ordering on the Pub/Sub topic.

Key responsibilities:
- Load runtime configuration (rate, distributions, toggles) from YAML + env.
- Build lifecycles via `generate_parcel_lifecycle(...)` (strictly increasing timestamps).
- Validate every event against the envelope and type-specific JSON Schemas.
- Publish to Pub/Sub using attributes (event_type, schema_version, event_version).

Endpoints:
- GET  /health
- POST /generate?eps=<int>&minutes=<int>
"""

import os
import time
from typing import List, Dict, Any

from fastapi import FastAPI, Query, HTTPException

from .config import load_config
from .schemas import SchemaRegistry
from .utils import utcnow, poisson_knuth, exponential_gaps
from .generator import generate_parcel_lifecycle
from .publisher import PubSubPublisher


def create_app() -> FastAPI:
    """
    Application factory: wires configuration, schema validation, and Pub/Sub publisher.

    Returns
    FastAPI
        A fully configured FastAPI application exposing:
          - GET  /health
          - POST /generate

    Layout & dependencies
    - Config directory:   ../configs        (default.yaml + <ENV>.yaml)
    - Schemas directory:  ../data_contracts/schemas (envelope + per-event schemas)
    - Environment vars:
        * GENERATOR_ENV      -> selects configs/<env>.yaml overlay (default: "dev")
        * GEN_EVENTS_PER_SEC -> overrides rate.events_per_sec in config (optional)
        * PUBSUB_TOPIC       -> overrides cfg.pubsub.topic (optional)
        * PROJECT_ID         -> GCP project for Pub/Sub (default: "relay-analytics-demo")
    """
    app = FastAPI(title="Relay Generator", version="1.0.0")

    # Resolve absolute paths for configs and schemas relative to this file.
    APP_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # -> <project>/generator
    CONFIG_DIR = os.path.join(APP_DIR, "configs")
    SCHEMA_DIR = os.path.join(APP_DIR, "data_contracts", "schemas")


    # Configuration & schema registry
    # Load default + environment overlay (and allow a couple of env overrides).
    cfg = load_config(os.path.abspath(CONFIG_DIR))

    # Prepare a schema registry that validates the envelope and event-type schemas.
    registry = SchemaRegistry(os.path.abspath(SCHEMA_DIR))

    # Pub/Sub publisher
    project_id = os.getenv("PROJECT_ID", "relay-analytics-demo")
    topic = os.getenv("TOPIC", cfg["pubsub"]["topic"])
    publisher = PubSubPublisher(project_id, topic)

    @app.get("/health")
    def health():
        """
        Lightweight readiness endpoint.

        Returns
        dict
            Example: {"ok": True, "topic": "parcel-events", "project_id": "relay-analytics-demo"}
        """
        return {"ok": True, "topic": topic, "project_id": project_id}

    @app.post("/generate")
    def generate(
        eps: int = Query(
            40,
            ge=1,
            le=200,
            description=(
                "Target events per second (Poisson mean). "
                "This controls how many **lifecycles** start per second; "
                "each lifecycle emits multiple events."
            ),
        ),
        minutes: int = Query(
            1,
            ge=1,
            le=10,
            description=(
                "How long to generate for (wall-clock minutes). "
                "Keep within Cloud Run request timeout unless that timeout is increased."
            ),
        ),
    ):
        """
        Stream parcel lifecycles for a bounded duration.

        The loop advances in ~1-second ticks. For each tick, we draw K ~ Poisson(eps) to decide
        how many lifecycles to start. Each lifecycle is built with strictly increasing timestamps,
        validated against JSON Schemas, and then published to Pub/Sub with `ordering_key = parcel_id`.

        Parameters
        eps : int, query
            Mean lifecycles per second (Poisson rate λ). Each lifecycle produces multiple events.
        minutes : int, query
            Number of minutes to run the generator loop for.

        Returns
        dict
            {"status": "ok", "eps": <int>, "minutes": <int>}

        Notes
        - Per-second jitter: we space out starts using exponential inter-arrival gaps to avoid bursts.
        - Validation: any schema violation would raise, halting the request with 422/500 depending on origin.
        - Backpressure: Pub/Sub client calls are awaited (future.result()) in the publisher class to ensure
          publish ordering guarantees hold and transient errors raise immediately in this demo service.
        """
        # Basic guardrail: extremely large requests are likely misconfigurations.
        if eps * minutes > 2000 * 10:  # soft sanity threshold
            pass

        end_time = time.time() + (minutes * 60)

        while time.time() < end_time:
            # Draw how many **lifecycles** to create this tick (Poisson with mean = eps).
            lam = float(eps)
            k = poisson_knuth(lam)

            # Build K lifecycles and pre-validate their events against JSON Schemas.
            lifecycles: List[List[Dict[str, Any]]] = []
            base_now = utcnow()
            for _ in range(max(0, k)):
                lifecycle = generate_parcel_lifecycle(base_now, cfg)

                # Validate every event before publish to fail-fast on contract drift.
                for evt in lifecycle:
                    import json
                    print(json.dumps(evt, indent=2))
                    registry.validate(evt)
                lifecycles.append(lifecycle)

            # Spread lifecycle starts within the current second using exponential gaps
            # (mean 1/eps seconds). This keeps the stream closer to a Poisson process.
            gaps = exponential_gaps(k, rate_per_sec=max(1.0, eps))

            # Publish all events; we stagger lifecycles with tiny sleeps to reduce bursts,
            # but maintain strict per-parcel event order by publishing lifecycle events sequentially.
            for idx, lifecycle in enumerate(lifecycles):
                if idx < len(gaps):
                    # Keep sleeps short so the request remains responsive in Cloud Run.
                    time.sleep(min(gaps[idx], 0.5))
                for evt in lifecycle:
                    publisher.publish(evt)

            # Small sleep to cap loop frequency; the Poisson timing already introduces variability.
            time.sleep(0.25)

        return {"status": "ok", "eps": eps, "minutes": minutes}

    return app
