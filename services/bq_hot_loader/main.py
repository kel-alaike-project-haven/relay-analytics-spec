# services/bq_hot_loader/main.py
import threading
import json
import os
from typing import Optional
from fastapi import FastAPI
import uvicorn

from app.subscriber import PubSubSubscriber

app = FastAPI()
_subscriber: Optional[PubSubSubscriber] = None
_worker: Optional[threading.Thread] = None

@app.on_event("startup")
def _startup():
    global _subscriber, _worker
    app.logger if hasattr(app, "logger") else None
    print("[bq-hot-loader] startup: creating PubSubSubscriber and background thread...")
    if _subscriber is None:
        _subscriber = PubSubSubscriber()
        _worker = threading.Thread(target=_subscriber.listen, daemon=True, name="pubsub-listener")
        _worker.start()
        print("[bq-hot-loader] background listener thread started.")

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
