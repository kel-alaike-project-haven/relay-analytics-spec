{{ config(
    materialized='view',
    alias='stg_merchant_attributes'
) }}

-- LATEST MERCHANT ATTRIBUTES DERIVED FROM EVENTS
with ranked as (
    select
        merchant_id,
        service_tier,
        promised_window_start,
        promised_window_end,
        event_ts,
        row_number() over (partition by merchant_id order by event_ts desc) as rn
    from {{ ref('silver_parcel_events') }}
    where merchant_id is not null
)
select
    merchant_id,
    service_tier,
    promised_window_start,
    promised_window_end,
    event_ts as last_seen_ts
from ranked
where rn = 1
