{{ config(
    materialized='table',
    schema='relay_gold',
    alias='dim_depot',
    cluster_by=['depot_id']
) }}

-- SIMPLE DEPOT LOOKUP FROM EVENTS
with stats as (
  select
    depot_id,
    min(event_ts) as first_seen_ts,
    max(event_ts) as last_seen_ts,
    countif(event_type = 'SCAN_IN_DEPOT') as scan_in_count,
    countif(event_type = 'SCAN_OUT_DEPOT') as scan_out_count
  from {{ ref('silver_parcel_events') }}
  where depot_id is not null
  group by depot_id
)
select
  depot_id as depot_sk,
  depot_id,
  first_seen_ts,
  last_seen_ts,
  scan_in_count,
  scan_out_count
from stats
