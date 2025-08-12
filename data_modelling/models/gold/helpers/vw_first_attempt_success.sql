-- Helper: one row per parcel summarizing delivery success
-- Notes:
-- - delivered_any: whether parcel delivered at all
-- - first_attempt_success: whether first attempt delivered
-- - Grain: parcel_id (+ useful dimension keys carried forward)

{{ config(
    materialized='view',
    tags=['gold','helper']
) }}

WITH attempts AS (
    SELECT
        parcel_id,
        depot_id,
        courier_id,
        merchant_id,
        attempt_number,
        outcome,
        -- Use attempt_ts when available, else event_date from the fact
        COALESCE(DATE(attempt_ts), event_date) AS event_date
    FROM {{ ref('fact_parcel_delivery_attempts') }}
),

by_parcel AS (
    SELECT
        parcel_id,
        ANY_VALUE(depot_id) AS depot_id,
        ANY_VALUE(courier_id) AS courier_id,
        ANY_VALUE(merchant_id) AS merchant_id,
        ANY_VALUE(event_date) AS event_date,

        -- Delivered_any: did this parcel ever have an outcome of 'DELIVERED'?
        MAX(CASE WHEN UPPER(outcome) = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_any,

        -- First_attempt_success: was attempt_number=1 AND outcome='DELIVERED'?
        MAX(CASE WHEN attempt_number = 1 AND UPPER(outcome) = 'DELIVERED' THEN 1 ELSE 0 END) AS first_attempt_success
    FROM attempts
    GROUP BY parcel_id
)

SELECT
    parcel_id,
    depot_id,
    courier_id,
    merchant_id,
    event_date,
    delivered_any,
    first_attempt_success
FROM by_parcel

