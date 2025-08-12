{% snapshot snap_merchant_scd2 %}
{{
  config(
    target_schema='relay_gold',
    unique_key='merchant_id',
    strategy='check',
    check_cols=['service_tier', 'promised_window_start', 'promised_window_end'],
    invalidate_hard_deletes=True
  )
}}

select
  merchant_id,
  service_tier,
  promised_window_start,
  promised_window_end,
  last_seen_ts
from {{ ref('stg_merchant_attributes') }}

{% endsnapshot %}
