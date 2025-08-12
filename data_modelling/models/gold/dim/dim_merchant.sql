{{ config(
    materialized='view',
    schema='relay_gold',
    alias='dim_merchant'
) }}

-- current merchant rows only
select
  merchant_id,
  service_tier,
  promised_window_start,
  promised_window_end,
  last_seen_ts,
  dbt_valid_from,
  dbt_valid_to
from {{ ref('snap_merchant_scd2') }}
where dbt_valid_to is null
