{{ config(
    materialized='table',
    schema='relay_gold',
    alias='dim_exception_code'
) }}

-- DISTINCT EXCEPTION CODES AND HINTS
select
  exception_code as exception_code_sk,
  exception_code,
  any_value(stage_hint) as stage_hint_example,
  count(*) as occurrences
from {{ ref('silver_parcel_events') }}
where exception_code is not null
group by exception_code
