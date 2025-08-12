{{ config(
    materialized='view',
    alias='stg_courier_attributes'
) }}

-- LATEST COURIER ATTRIBUTES DERIVED FROM EVENTS
-- If you later ingest a proper courier master, swap this to that source.
with base as (
    select
        courier_id,
        any_value(route_id) as sample_route_id,            -- placeholder attribute
        max(event_ts) as last_seen_ts
    from {{ ref('silver_parcel_events') }}
    where courier_id is not null
    group by courier_id
)

select
    courier_id,
    sample_route_id,
    last_seen_ts
from base
