/*SILVER LAYER: Conformed, typed, de-duplicated parcel events.
- Removes duplicates using event_id
- Applies latest event per (parcel_id, event_type)
- Clusters by parcel_id, event_type
- Partitions by event_date for efficient querying*/

{{ config(
    materialized='incremental',
    alias='silver_parcel_events',
    partition_by={
        "field": "event_date",
        "data_type": "date"
    },
    cluster_by=['parcel_id', 'event_type'],
    unique_key='event_id'
) }}

WITH source_data AS (

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
        DATE(event_ts) AS event_date
    FROM {{ source('relay_bronze', 'parcel_events') }}
    WHERE DATE(event_ts) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('bootstrap_days', 7) }} DAY) AND CURRENT_DATE()
    {% if is_incremental() %}
        -- process only new/updated records in incremental runs
        AND event_ts > (SELECT max(event_ts) FROM {{ this }})
    {% endif %}

),

-- Deduplicate by event_id (keep latest by event_ts)
deduplicated AS (
    SELECT
        ranked.schema_version,
        ranked.event_version,
        ranked.event_id,
        ranked.parcel_id,
        ranked.event_type,
        ranked.event_ts,
        ranked.producer,
        ranked.sequence_no,
        ranked.trace_id,
        ranked.merchant_id,
        ranked.origin_address_id,
        ranked.destination_address_id,
        ranked.service_tier,
        ranked.created_ts,
        ranked.promised_window_start,
        ranked.promised_window_end,
        ranked.weight_grams,
        ranked.volume_cm3,
        ranked.depot_id,
        ranked.scanner_id,
        ranked.area_code,
        ranked.belt_no,
        ranked.route_id,
        ranked.courier_id,
        ranked.planned_stop_seq,
        ranked.exception_code,
        ranked.stage_hint,
        ranked.details,
        ranked.first_planned_eta_ts,
        ranked.predicted_delivery_ts,
        ranked.generated_ts,
        ranked.source,
        ranked.delivered_ts,
        ranked.attempt_number,
        ranked.outcome,
        ranked.failure_reason,
        ranked.event_date
    FROM (
        SELECT
            sd.schema_version,
            sd.event_version,
            sd.event_id,
            sd.parcel_id,
            sd.event_type,
            sd.event_ts,
            sd.producer,
            sd.sequence_no,
            sd.trace_id,
            sd.merchant_id,
            sd.origin_address_id,
            sd.destination_address_id,
            sd.service_tier,
            sd.created_ts,
            sd.promised_window_start,
            sd.promised_window_end,
            sd.weight_grams,
            sd.volume_cm3,
            sd.depot_id,
            sd.scanner_id,
            sd.area_code,
            sd.belt_no,
            sd.route_id,
            sd.courier_id,
            sd.planned_stop_seq,
            sd.exception_code,
            sd.stage_hint,
            sd.details,
            sd.first_planned_eta_ts,
            sd.predicted_delivery_ts,
            sd.generated_ts,
            sd.source,
            sd.delivered_ts,
            sd.attempt_number,
            sd.outcome,
            sd.failure_reason,
            sd.event_date,
            ROW_NUMBER() OVER (
                PARTITION BY sd.event_id
                ORDER BY sd.event_ts DESC
            ) AS rn
        FROM source_data AS sd
    ) AS ranked
    WHERE ranked.rn = 1
),

-- Latest event per (parcel_id, event_type) by event_ts
latest_event_per_type AS (
    SELECT
        ranked.schema_version,
        ranked.event_version,
        ranked.event_id,
        ranked.parcel_id,
        ranked.event_type,
        ranked.event_ts,
        ranked.producer,
        ranked.sequence_no,
        ranked.trace_id,
        ranked.merchant_id,
        ranked.origin_address_id,
        ranked.destination_address_id,
        ranked.service_tier,
        ranked.created_ts,
        ranked.promised_window_start,
        ranked.promised_window_end,
        ranked.weight_grams,
        ranked.volume_cm3,
        ranked.depot_id,
        ranked.scanner_id,
        ranked.area_code,
        ranked.belt_no,
        ranked.route_id,
        ranked.courier_id,
        ranked.planned_stop_seq,
        ranked.exception_code,
        ranked.stage_hint,
        ranked.details,
        ranked.first_planned_eta_ts,
        ranked.predicted_delivery_ts,
        ranked.generated_ts,
        ranked.source,
        ranked.delivered_ts,
        ranked.attempt_number,
        ranked.outcome,
        ranked.failure_reason,
        ranked.event_date
    FROM (
        SELECT
            d.schema_version,
            d.event_version,
            d.event_id,
            d.parcel_id,
            d.event_type,
            d.event_ts,
            d.producer,
            d.sequence_no,
            d.trace_id,
            d.merchant_id,
            d.origin_address_id,
            d.destination_address_id,
            d.service_tier,
            d.created_ts,
            d.promised_window_start,
            d.promised_window_end,
            d.weight_grams,
            d.volume_cm3,
            d.depot_id,
            d.scanner_id,
            d.area_code,
            d.belt_no,
            d.route_id,
            d.courier_id,
            d.planned_stop_seq,
            d.exception_code,
            d.stage_hint,
            d.details,
            d.first_planned_eta_ts,
            d.predicted_delivery_ts,
            d.generated_ts,
            d.source,
            d.delivered_ts,
            d.attempt_number,
            d.outcome,
            d.failure_reason,
            d.event_date,
            ROW_NUMBER() OVER (
                PARTITION BY d.parcel_id, d.event_type
                ORDER BY d.event_ts DESC
            ) AS rn
        FROM deduplicated AS d
    ) AS ranked
    WHERE ranked.rn = 1
)

SELECT
    schema_version,
    event_version,
    event_id,
    parcel_id,
    event_type,
    event_ts,
    producer,
    sequence_no,
    trace_id,
    merchant_id,
    origin_address_id,
    destination_address_id,
    service_tier,
    created_ts,
    promised_window_start,
    promised_window_end,
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
    first_planned_eta_ts,
    predicted_delivery_ts,
    generated_ts,
    source,
    delivered_ts,
    attempt_number,
    outcome,
    failure_reason,
    event_date
FROM latest_event_per_type