-- models/silver/silver_parcel_latest.sql
-- ============================================================
-- SILVER LAYER (snapshot): latest event per parcel_id
-- - Derives FROM silver_parcel_events (already deduped + conformed)
-- - Keeps only the most recent event for each parcel_id
-- - Incremental with merge ON parcel_id:
--     * On full refresh: compute latest for all parcels
--     * On incremental runs: recompute only parcels that changed
-- - Partitioned BY event_date; clustered BY parcel_id
-- ============================================================

{{ config(
    materialized='incremental',
    alias='silver_parcel_latest',
    unique_key='parcel_id',
    incremental_strategy='merge',
    partition_by={
        "field": "event_date",
        "data_type": "date"
    },
    cluster_by=['parcel_id']
) }}

WITH base AS (
    -- source is the conformed/deduped silver events
    SELECT
        schema_version,
        event_version,
        event_id,
        parcel_id,
        event_type,
        CAST(event_ts AS TIMESTAMP) AS event_ts,
        producer,
        sequence_no,
        trace_id,
        merchant_id,
        origin_address_id,
        destination_address_id,
        service_tier,
        CAST(created_ts AS TIMESTAMP) AS created_ts,
        CAST(promised_window_start AS TIMESTAMP) AS promised_window_start,
        CAST(promised_window_end AS TIMESTAMP) AS promised_window_end,
        weight_grams,
        volume_cm3,
        depot_id,
        scanner_id,
        area_code,
        belt_no,
        route_id,
        courier_id,
        planned_stop_seq,
        exception_code,
        stage_hint,
        details,
        CAST(first_planned_eta_ts AS TIMESTAMP) AS first_planned_eta_ts,
        CAST(predicted_delivery_ts AS TIMESTAMP) AS predicted_delivery_ts,
        CAST(generated_ts AS TIMESTAMP) AS generated_ts,
        source,
        CAST(delivered_ts AS TIMESTAMP) AS delivered_ts,
        attempt_number,
        outcome,
        failure_reason,
        date(event_ts) AS event_date
    FROM {{ ref('silver_parcel_events') }}
),

-- latest event for every parcel across all event types
latest_all AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.parcel_id
                ORDER BY b.event_ts desc
            ) AS rn
        FROM base AS b
    )
    WHERE rn = 1
),

{% if is_incremental() %}
-- n incremental runs, limit the merge set to parcels that had activity
-- since the last successful build of this table
changed_parcels AS (
    SELECT distinct parcel_id
    FROM {{ ref('silver_parcel_events') }}
    WHERE event_ts > coalesce((SELECT max(event_ts) FROM {{ this }}), TIMESTAMP('1970-01-01'))
),

final AS (
    SELECT la.*
    FROM latest_all la
    join changed_parcels cp
      ON la.parcel_id = cp.parcel_id
)
{% else %}
final AS (
    SELECT * FROM latest_all
)
{% endif %}

SELECT *
FROM final
