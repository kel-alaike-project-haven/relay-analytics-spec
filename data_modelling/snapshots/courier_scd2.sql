{% snapshot snap_courier_scd2 %}
{{
  config(
    target_schema='relay_gold',
    unique_key='courier_id',
    strategy='check',
    check_cols=['sample_route_id'],
    invalidate_hard_deletes=True
  )
}}

select
  courier_id,
  sample_route_id,
  last_seen_ts
from {{ ref('stg_courier_attributes') }}

{% endsnapshot %}
