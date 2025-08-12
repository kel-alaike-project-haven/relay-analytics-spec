# 📦 Ingestion Services

## Overview
This demo’s **event ingestion stack** is built to handle **both real-time and historical parcel event data** with high reliability, strong schema validation, and flexible deployment.  

The stack is composed of **three core services**:

1. **Parcel Lifecycle Generator** – Generates synthetic events for testing and development, simulating end-to-end parcel journeys.
2. **BQ Hot Loader** – Consumes parcel events from Pub/Sub and loads validated data into BigQuery for real-time analytics.
3. **GCS Cold Loader** – Validates and stores historical or batch event data in GCS as Avro files for archival or backfill processing.

## Full Architecture Design
<img width="2520" height="1760" alt="Relay _ Parcel Network Core Models - Architecture (2)" src="https://github.com/user-attachments/assets/d144eb87-7cd4-4333-b845-c781fa782b4f" />

---

## Event Flow
```text
[Generator / External Source] 
       ↓
   Pub/Sub Topic
       ↓
   ┌─────────────┬──────────────┐
   │  Hot Loader │  Cold Loader │
   │  (BigQuery) │   (GCS Avro) │
   └─────────────┴──────────────┘
```

- **Hot Loader Path:** Events are validated and inserted into BigQuery within seconds, enabling low-latency dashboards and analytics.
- **Cold Loader Path:** Events are validated and written to Avro in GCS, optimised for archival, large-scale reprocessing, and backfill.

---

## Deployment Pipeline

### **1. Containerisation**
All services are **packaged as Docker images**, ensuring consistent runtime environments and eliminating “works on my machine” issues.

### **2. Artifact Registry**
Each built image is **tagged and pushed to Artifact Registry**, which serves as the single source of truth for all service images.  
The **`:latest`** tag always reflects the most recent, tested code on the `main` branch.

### **3. Cloud Build Triggers**
Cloud Build is configured with **build triggers** for each service:
- When a change is merged into `main`:
  1. Cloud Build pulls the latest code.
  2. Builds the Docker image.
  3. Tags it with `:latest` (and optionally a commit SHA).
  4. Pushes it to Artifact Registry.
  5. Deploys the updated image to the respective **Cloud Run** service.

This ensures **fully automated CI/CD** – deployments happen within minutes of merging.

---

## Key Benefits
- **Schema-First Validation** – Every event is validated against JSON Schema before ingestion.
- **Separation of Concerns** – Real-time (BQ Hot Loader) and batch/archival (GCS Cold Loader) pipelines are fully decoupled.
- **Synthetic Testing Data** – The Parcel Lifecycle Generator allows testing transformations, KPIs, and dashboards without production dependencies.
- **Immutable & Traceable Deployments** – All builds are versioned and stored in Artifact Registry, enabling rollback to previous versions if needed.
- **Fully Automated Delivery** – No manual steps between commit and production deployment.

---

**Cloud Build Console:**  
[View builds in GCP Console](https://console.cloud.google.com/cloud-build/builds?hl=en&project=relay-analytics-demo)

**Full Write Up can be found here:**
[View full demo Write Up](https://docs.google.com/document/d/1zFLqC7SMvdXW7uNH0M8wRKbjYeRVnJM34D_lNLwxrJw/edit?usp=sharing)
