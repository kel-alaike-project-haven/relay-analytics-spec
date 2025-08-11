# main.py
import os
import time
import threading
import base64
import binascii
import json
from typing import Optional

from fastapi import FastAPI, Request
import uvicorn
from google.cloud import pubsub_v1

from app.validator import EventValidator
from app.loader import GCSAvroLoader
from app import config

app = FastAPI()

validator = EventValidator()
loader = GCSAvroLoader()

def _callback_factory(acked, nacked):
    def _cb(message: pubsub_v1.subscriber.message.Message):
        try:
            raw_bytes = message.data
            try:
                decoded = base64.b64decode(raw_bytes, validate=True)
                payload = decoded.decode("utf-8")
            except (binascii.Error, UnicodeDecodeError):
                payload = raw_bytes.decode("utf-8")

            data = json.loads(payload)
            validator.validate_event(data)
            path = loader.upload_event(data)
            print(f"Uploaded to {path}")
            message.ack()
            acked.append(1)
        except Exception as e:
            print(f"Error processing message: {e}")
            message.nack()
            nacked.append(1)
    return _cb

def _run_for(seconds: int, max_outstanding: int = 100) -> dict:
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(config.PROJECT_ID, config.SUBSCRIPTION_ID)
    acked, nacked = [], []
    future = subscriber.subscribe(subscription_path, callback=_callback_factory(acked, nacked),
                                  flow_control=pubsub_v1.types.FlowControl(max_messages=max_outstanding))
    print(f"Listening for {seconds}s on {subscription_path}...")

    try:
        time.sleep(seconds)
    finally:
        future.cancel()
        # ensure graceful stop
        try:
            future.result(timeout=10)
        except Exception:
            pass
        subscriber.close()

    return {"acked": len(acked), "nacked": len(nacked), "duration_sec": seconds}

@app.get("/health")
def healthz():
    return {"status": "ok"}

@app.post("/run")
async def run(request: Request):
    body: Optional[dict] = None
    try:
        body = await request.json()
    except Exception:
        body = {}
    seconds = int((body or {}).get("seconds", os.getenv("RUN_SECONDS", 300)))
    max_outstanding = int((body or {}).get("max_outstanding", os.getenv("MAX_OUTSTANDING", 100)))
    result = _run_for(seconds=seconds, max_outstanding=max_outstanding)
    return {"status": "completed", **result}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
