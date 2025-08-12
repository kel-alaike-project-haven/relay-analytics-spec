-- FACT: One row per ETA set/updated event
-- Grain: event_id (ETA event)
-- Used for: ETA MAE, ETA p95 error

{{ config(
    alias='fact_eta_events',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partition_by={"field": "event_date", "data_type": "date"},
    cluster_by=['parcel_id','route_id','source']
) }}

WITH base AS (
    SELECT
        event_id,
        parcel_id,
        route_id,
        depot_id,
        courier_id,
        merchant_id,
        source,
        CAST(first_planned_eta_ts AS TIMESTAMP) AS first_planned_eta_ts,
        CAST(predicted_delivery_ts AS TIMESTAMP) AS predicted_delivery_ts,
        CAST(generated_ts AS TIMESTAMP) AS generated_ts,
        CAST(event_ts AS TIMESTAMP) AS event_ts,
        DATE(event_ts) AS event_date
    FROM {{ ref('silver_parcel_events') }}
    WHERE DATE(event_ts) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('bootstrap_days', 7) }} DAY) AND CURRENT_DATE()
      AND event_type IN ('ETA_SET','ETA_UPDATED')
      -- Keep rows that can support ETA error metrics
      AND predicted_delivery_ts IS NOT NULL
      AND generated_ts IS NOT NULL
    {% if is_incremental() %}
      AND DATE(event_ts) > (SELECT IFNULL(MAX(event_date), DATE '1970-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    event_id,
    parcel_id,
    route_id,
    depot_id,
    courier_id,
    merchant_id,
    source,
    first_planned_eta_ts,
    predicted_delivery_ts,
    generated_ts,
    event_ts,
    event_date
FROM base
