{{ config(
    materialized='view',
    schema='relay_gold',
    alias='dim_courier'
) }}

select
  courier_id,
  sample_route_id,
  last_seen_ts,
  dbt_valid_from,
  dbt_valid_to
from {{ ref('snap_courier_scd2') }}
where dbt_valid_to is null
