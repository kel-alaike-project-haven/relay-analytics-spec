# 📦 Parcel Lifecycle Generator

## Overview
The **Parcel Lifecycle Generator** is a synthetic event producer designed to simulate a realistic parcel delivery journey.  
It generates **strictly ordered JSON events** that conform to the JSON Schemas defined in `data_contracts/` and publishes them to **Google Cloud Pub/Sub**.

This service can:
- Produce realistic, schema-compliant event sequences
- Inject configurable exceptions and delays
- Maintain **strict ordering per parcel** using Pub/Sub message ordering
- Run locally for testing or deploy to **Cloud Run** using Docker

---

## 📜 Example Event Flow

A typical parcel lifecycle (with optional exceptions) follows:

1. **PARCEL_CREATED**  
2. **SCAN_IN_DEPOT** (+ possible depot exceptions)
3. **SCAN_OUT_DEPOT**  
4. **LOADED_TO_VAN** (+ possible breakdown)  
5. **OUT_FOR_DELIVERY**  
6. **ETA_SET**  
7. **ETA_UPDATED** (0–2 times)  
8. **DELIVERED** (possibly multiple attempts)  

---

## 📂 Project Structure

```
services/generator/
├── app/
│   ├── __init__.py           # App factory
│   ├── main.py               # Entry point (uvicorn calls this)
│   ├── api.py                # FastAPI endpoints
│   ├── publisher.py          # Pub/Sub publisher logic
│   ├── generator.py          # Parcel lifecycle generation logic
│   ├── utils.py              # Common helper functions (UUIDs, timestamps, clamps)
│   └── config.yaml           # Runtime config (schema version, lifecycle timings, exceptions)
│
├── data_contracts/
│   ├── event-envelope.schema.json
│   ├── delivered.schema.json
│   ├── scan_in_depot.schema.json
│   ├── ... (other event schemas)
│
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## ⚙️ Prerequisites

- **Python** 3.9+  
- **pip** / **pipenv** (recommended)  
- **Google Cloud SDK** (`gcloud`)  
- A **GCP Project** with:
  - Pub/Sub API enabled
  - A Pub/Sub **topic** for parcel events
  - A Pub/Sub **subscription** with message ordering enabled

---

## 🔧 Local Development

### 1. Clone the repository
```bash
git clone https://github.com/<your-org>/relay-analytics-spec.git
cd services/generator
```

### 2. Create a virtual environment & install dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Authenticate with GCP  
You can use **Application Default Credentials** (ADC) for local development:
```bash
gcloud auth application-default login
```
This removes the need to hardcode a JSON key path.

---

## 🚀 Running Locally

### Using Uvicorn (recommended for dev):
```bash
uvicorn app.main:create_app --factory --reload --port 8000
```

- `--factory` tells Uvicorn to call the `create_app()` function.
- `--reload` watches for file changes.
- Tracebacks will appear in your terminal if anything fails.

---

## 📡 Testing the Generator Locally

Once running, hit the **generate endpoint**:

```bash
curl -X POST "http://127.0.0.1:8000/generate"      -H "Content-Type: application/json"      -d '{
           "count": 5,
           "config": {
             "schema": { "version": "1.0.0" },
             "lifecycle": {
               "in_depot_min": 1, "in_depot_max": 5,
               "out_depot_min": 2, "out_depot_max": 6,
               "loaded_min": 1, "loaded_max": 3,
               "ofd_min": 2, "ofd_max": 4
             },
             "exceptions": {
               "MISSORT": 0.1,
               "DEPOT_CAPACITY": 0.05,
               "VEHICLE_BREAKDOWN": 0.02,
               "ADDRESS_ISSUE": 0.02,
               "CUSTOMER_NOT_HOME": 0.05
             },
             "eta": { "mean_minutes": 90, "sd_minutes": 15, "update_prob": 0.5 }
           }
         }'
```

This will:
- Generate 5 parcel lifecycles
- Publish each event to Pub/Sub (topic from `config.yaml`)
- Return a success response once published

---

## 📬 Pub/Sub Setup

### Create topic & subscription (with ordering enabled)
```bash
TOPIC=parcel-events
SUB=sub-hot-bq

gcloud pubsub topics create $TOPIC

gcloud pubsub subscriptions create $SUB   --topic=$TOPIC   --enable-message-ordering
```

---

## ☁️ Deploy to Cloud Run

### 1. Build & push image
```bash
gcloud builds submit --tag gcr.io/<PROJECT_ID>/parcel-generator
```

### 2. Deploy
```bash
gcloud run deploy parcel-generator   --image gcr.io/<PROJECT_ID>/parcel-generator   --platform managed   --region europe-west2   --allow-unauthenticated
```

---

## 🛠 Environment Variables

| Variable            | Description |
|---------------------|-------------|
| `PROJECT_ID`        | GCP Project ID |
| `PUBSUB_TOPIC`      | Pub/Sub topic name |
| `PUBSUB_ORDERING`   | `true` to enable ordering |
| `CONFIG_PATH`       | Path to YAML config file |

---

## 🧪 Testing in Cloud Run

Once deployed, hit the public endpoint:
```bash
curl -X POST "https://<your-service>.run.app/generate"      -H "Content-Type: application/json"      -d '{"count": 3, "config": {...}}'
```

---

## 📌 Notes

- **Ordering keys** use `parcel_id` to guarantee sequence within each parcel.
- All events conform to **Draft 2020-12 JSON Schema** contracts.
- Failure reasons are **always included** for `DELIVERED` events (set to `"N/A"` if not applicable).
- Randomness is deterministic in dev (seeded), ensuring reproducible runs.
