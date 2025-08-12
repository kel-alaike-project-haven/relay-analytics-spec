
# GCS Cold Loader

## Overview
The **GCS Cold Loader** ingests **historical or batch event data** into **Google Cloud Storage (GCS)** in **Avro format** after validating against the defined JSON Schemas.  
It is optimised for **large-scale backfills** and **archival storage**.

## Project Structure
services/cold_loader/
- app/
  - main.py      
  - loader.py           
  - validator.py          
- data_contracts/       
- Dockerfile
- requirements.txt
- README.md

## Prerequisites
- Python 3.9+
- Google Cloud SDK installed
- GCP Project with:
  - GCS bucket created
  - Pub/Sub topic & subscription for backfill
  - Avro format support enabled in downstream tools

## Running Locally
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
gcloud auth application-default login

## Features
- Writes one Avro file per event
- Partitions by events/YYYY/MM/DD/HH/
- Validates against JSON Schemas
- Auto-fills missing fields as NULL

## Assumptions
- No deduplication logic
- All schemas exist locally
- Perfect ordering assumed
