-- Helper: daily p95 depot dwell (minutes) by depot
-- Notes:
-- - Grain: (event_date, depot_id)
-- - Backed by fact_depot_visits

{{ config(
    materialized='view',
    tags=['gold','helper']
) }}

SELECT
    event_date,
    depot_id,
    APPROX_QUANTILES(dwell_minutes, 100)[OFFSET(95)] AS depot_dwell_p95_min
FROM {{ ref('fact_depot_visits') }}
GROUP BY
    event_date,
    depot_id
