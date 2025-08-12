
# BigQuery Hot Loader

## Overview
The **BQ Hot Loader** ingests **real-time parcel events** from **Pub/Sub** directly into **BigQuery**, enforcing schema conformance before insert.

## Project Structure
services/hot_loader/
- app/
  - main.py             
  - subscriber.py      
  - validator.py         
  - loader.py          
- data_contracts/
- Dockerfile
- requirements.txt
- README.md

## Prerequisites
- Python 3.9+
- Google Cloud SDK installed
- GCP Project with:
  - Pub/Sub topic & subscription for live events
  - BigQuery dataset & tables created

## Running Locally
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
gcloud auth application-default login

python app/main.py --subscription sub-hot-loader --bq_dataset relay_gold

## Features
- Low-latency ingestion (<5s from publish)
- Schema-driven validation
- Inserts directly into partitioned BQ tables

## Assumptions
- Every event_type has a schema
- Malformed events are NACKed for retry
- Strict envelope structure assumed
