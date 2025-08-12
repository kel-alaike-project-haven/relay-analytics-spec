# 📊 DBT Modelling

## Overview
This repository contains the **dbt** project for transforming and modelling parcel delivery data in BigQuery.  
The models are organised into **bronze, silver, and gold** layers, each serving a distinct purpose in the ELT pipeline.

## Layered Architecture
- **Bronze** – Raw ingested data from source systems or synthetic event generators. Minimal transformations.
- **Silver** – Cleansed and conformed data with standardised formats, enriched dimensions, and derived fields.
- **Gold** – Business-ready fact and dimension tables used for analytics, KPI dashboards, and Looker Studio reporting.

## Directory Structure
```
dbt_project/
├── models/
│   ├── bronze/          # Source-aligned raw tables
│   ├── silver/          # Cleaned and conformed models
│   ├── gold/            # Analytics-ready facts & dimensions
│   │   ├── facts/
│   │   ├── dims/
│   │   └── helpers/
│   └── marts/           # Subject area marts
├── tests/               # Schema & data tests
├── macros/              # Custom dbt macros
├── seeds/               # Static CSV datasets
├── snapshots/           # Historical tracking of slowly changing dimensions
└── dbt_project.yml      # Project configuration
```

## Modelling Approach
We follow a **layered modelling approach**:
1. **Bronze** – Directly maps to ingested schema, using `source()` references.
2. **Silver** – Applies cleansing, type casting, and schema conformity.
3. **Gold** – Derives metrics and joins with dimensions to create business-friendly datasets.

### Example Flow
```
silver_parcel_events (silver)
    ↓ Cleansing & enrichment
fact_parcel_delivery_attempts (gold/fact)
    ↓ Aggregation & KPI derivations
vw_first_attempt_success (gold/helper)
    ↓ Consumed in Looker Studio for dashboards
```

## Testing Strategy
We implement **tests at all layers**:
- **Source freshness** checks on bronze tables.
- **Schema tests** for expected columns and datatypes.
- **Custom tests** for SLA breaches, invalid statuses, and delivery attempt rules.

Example test in `schema.yml`:
```yaml
tests:
  - unique:
      column_name: event_id
  - not_null:
      column_name: parcel_id
```

## Incremental Models
Where appropriate, gold fact tables are **incremental** to optimise cost and performance.  
We use:
```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partition_by={"field": "event_date", "data_type": "date"},
    cluster_by=['parcel_id','route_id','courier_id']
) }}
```
Incremental models only process new data beyond the last `event_date` already loaded.

## Deployment
This dbt project can be run via:
```bash
dbt run --profiles-dir .
dbt test --profiles-dir .
```
In production, it's triggered through **Cloud Composer / Airflow** for scheduled refreshes.

## Some metrics that can be created from this data
The gold layer feeds metrics such as:
- On-time delivery rate
- First attempt success rate
- ETA MAE & p95 error
- Depot dwell p95
- SLA breach rate

These metrics are calculated from helper and fact models.

## Key Benefits
- **Reproducible** transformations
- **Tested** datasets at every stage
- **Optimised** BigQuery cost via incremental loads
- **Business-ready** tables for direct consumption
