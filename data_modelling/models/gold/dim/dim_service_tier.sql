{{ config(
    materialized='table',
    schema='relay_gold',
    alias='dim_service_tier'
) }}

-- DISTINCT SERVICE TIERS WITH SAMPLE PROMISED WINDOWS
with base as (
  select
    service_tier,
    any_value(promised_window_start) as sample_promised_start,
    any_value(promised_window_end) as sample_promised_end
  from {{ ref('silver_parcel_events') }}
  where service_tier is not null
  group by service_tier
)
select
  service_tier as service_tier_sk,
  service_tier,
  sample_promised_start,
  sample_promised_end
from base
