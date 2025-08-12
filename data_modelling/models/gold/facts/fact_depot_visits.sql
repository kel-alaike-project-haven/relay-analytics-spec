-- FACT: One row per depot stay (scan-in → scan-out pair)
-- Grain: parcel_id + depot_id + in_depot_ts

{{ config(
    alias='fact_depot_visits',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['parcel_id','depot_id','in_depot_ts'],
    partition_by={"field": "event_date", "data_type": "date"},
    cluster_by=['parcel_id','depot_id']
) }}

WITH depot_scans AS (
    SELECT
        event_id,
        parcel_id,
        depot_id,
        event_type,
        CAST(event_ts AS TIMESTAMP) AS event_ts,
        DATE(event_ts) AS event_date
    FROM {{ ref('silver_parcel_events') }}
    WHERE DATE(event_ts) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('bootstrap_days', 7) }} DAY) AND CURRENT_DATE()
      AND event_type IN ('SCAN_IN_DEPOT','SCAN_OUT_DEPOT')
      AND depot_id IS NOT NULL
    {% if is_incremental() %}
      AND DATE(event_ts) > (SELECT IFNULL(MAX(event_date), DATE '1970-01-01') FROM {{ this }})
    {% endif %}
),

paired AS (
    SELECT
        s.parcel_id,
        s.depot_id,
        s.event_date,
        s.event_ts AS in_depot_ts,
        LEAD(s.event_ts) OVER (PARTITION BY s.parcel_id, s.depot_id ORDER BY s.event_ts) AS out_depot_ts,
        s.event_type AS curr_type,
        LEAD(s.event_type) OVER (PARTITION BY s.parcel_id, s.depot_id ORDER BY s.event_ts) AS next_type
    FROM depot_scans AS s
),

visits AS (
    SELECT
        parcel_id,
        depot_id,
        event_date,
        in_depot_ts,
        out_depot_ts,
        -- INT64 minutes
        TIMESTAMP_DIFF(out_depot_ts, in_depot_ts, MINUTE) AS dwell_minutes
    FROM paired
    WHERE curr_type = 'SCAN_IN_DEPOT'
      AND next_type = 'SCAN_OUT_DEPOT'
      AND out_depot_ts IS NOT NULL
)

SELECT
    parcel_id,
    depot_id,
    in_depot_ts,
    out_depot_ts,
    dwell_minutes,
    DATE(in_depot_ts) AS event_date
FROM visits
