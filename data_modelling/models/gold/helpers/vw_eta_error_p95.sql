-- Helper: daily p95 ETA absolute error (minutes) by depot/courier/merchant
-- Notes:
-- - Uses APPROX_QUANTILES for scalable p95
-- - Grain: (event_date, depot_id, courier_id, merchant_id)
-- - Backed by fact_eta_events

{{ config(
    materialized='view',
    tags=['gold','helper']
) }}

WITH base AS (
    SELECT
        DATE(generated_ts) AS event_date,
        depot_id,
        courier_id,
        merchant_id,
        ABS(TIMESTAMP_DIFF(first_planned_eta_ts, predicted_delivery_ts, MINUTE)) AS eta_error_minutes
    FROM {{ ref('fact_eta_events') }}
    WHERE event_ts IS NOT NULL
      AND predicted_delivery_ts IS NOT NULL
)

SELECT
    event_date,
    depot_id,
    courier_id,
    merchant_id,
    APPROX_QUANTILES(eta_error_minutes, 100)[OFFSET(95)] AS eta_p95_error_min
FROM base
GROUP BY
    event_date,
    depot_id,
    courier_id,
    merchant_id
