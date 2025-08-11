# main.py
import threading
import json
import os
from fastapi import FastAPI
import uvicorn

from app.subscriber import PubSubSubscriber

app = FastAPI()
_subscriber: PubSubSubscriber | None = None
_worker: threading.Thread | None = None

@app.on_event("startup")
def _startup():
    global _subscriber, _worker
    if _subscriber is None:
        _subscriber = PubSubSubscriber()
        _worker = threading.Thread(target=_subscriber.listen, daemon=True)
        _worker.start()

@app.get("/health")
def healthz():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
