-- FACT: One row per delivery attempt (successful or not)
-- Grain: event_id (attempt event)
-- Used for: First attempt success rate, SLA breach attribution

{{ config(
    alias='fact_parcel_delivery_attempts',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partition_by={"field": "event_date", "data_type": "date"},
    cluster_by=['parcel_id','route_id','courier_id','attempt_number']
) }}

WITH base AS (
    SELECT
        event_id,
        parcel_id,
        route_id,
        courier_id,
        depot_id,
        merchant_id,
        attempt_number,
        outcome,
        failure_reason,
        CAST(event_ts AS TIMESTAMP) AS attempt_ts,
        DATE(event_ts) AS event_date
    FROM {{ ref('silver_parcel_events') }}
    WHERE DATE(event_ts) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('bootstrap_days', 7) }} DAY) AND CURRENT_DATE()
      AND event_type = 'DELIVERED'
      AND attempt_number IS NOT NULL
    {% if is_incremental() %}
      AND DATE(event_ts) > (SELECT IFNULL(MAX(event_date), DATE '1970-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    event_id,
    parcel_id,
    route_id,
    courier_id,
    depot_id,
    merchant_id,
    attempt_number,
    outcome,
    failure_reason,
    attempt_ts,
    event_date
FROM base
