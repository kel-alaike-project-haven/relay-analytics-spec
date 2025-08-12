-- FACT: One row per successful delivery event
-- Grain: event_id (delivery event)
-- Used for: On-time delivery rate, First attempt success rate

{{ config(
    alias='fact_parcel_deliveries',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partition_by={"field": "event_date", "data_type": "date"},
    cluster_by=['parcel_id','route_id','courier_id']
) }}

WITH base AS (
    SELECT
        event_id,
        parcel_id,
        route_id,
        courier_id,
        attempt_number,
        outcome,
        failure_reason,
        CAST(delivered_ts AS TIMESTAMP) AS delivered_ts,
        DATE(event_ts) AS event_date,
        event_ts
    FROM {{ ref('silver_parcel_events') }}
    WHERE DATE(event_ts) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('bootstrap_days', 7) }} DAY) AND CURRENT_DATE()
      AND event_type = 'DELIVERED'
      AND delivered_ts IS NOT NULL
    {% if is_incremental() %}
      AND DATE(event_ts) > (SELECT IFNULL(MAX(event_date), DATE '1970-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    event_id,
    parcel_id,
    route_id,
    courier_id,
    attempt_number,
    outcome,
    failure_reason,
    delivered_ts,
    event_date
FROM base
