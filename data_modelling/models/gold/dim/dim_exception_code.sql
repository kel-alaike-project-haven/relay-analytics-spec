{{ config(
    materialized='table',
    schema='relay_gold',
    alias='dim_exception_code'
) }}

-- Distinct exception codes and hints
select
  exception_code,
  any_value(stage_hint) as stage_hint_example,
  count(*) as occurrences
from {{ ref('silver_parcel_events') }}
where exception_code is not null
group by exception_code
