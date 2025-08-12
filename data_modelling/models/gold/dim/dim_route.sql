{{ config(
    materialized='table',
    schema='relay_gold',
    alias='dim_route',
    cluster_by=['route_id']
) }}

-- SIMPLE ROUTE LOOKUP
with stats as (
  select
    route_id,
    min(event_ts) as first_seen_ts,
    max(event_ts) as last_seen_ts,
    count(*) as event_count
  from {{ ref('silver_parcel_events') }}
  where route_id is not null
  group by route_id
)
select
  route_id as route_sk,
  route_id,
  first_seen_ts,
  last_seen_ts,
  event_count
from stats
