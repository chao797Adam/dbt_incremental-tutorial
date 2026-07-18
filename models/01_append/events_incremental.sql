{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='delta',
    partition_by='event_date',
    cluster_by=['user_id', 'event_name'],
    on_schema_change='fail'
  )
}}

-- APPEND Strategy on Databricks
-- Optimized for Delta Lake

SELECT
    event_id,
    user_id,
    event_name,
    event_timestamp,
    product_id,
    amount,
    event_date,
    CURRENT_TIMESTAMP() as processed_at

FROM {{ ref('events') }}

{% if is_incremental() %}
  -- Only process events since last run
  -- Add 10-minute buffer for clock skew
  WHERE event_timestamp > (
    SELECT TIMESTAMPADD(MINUTE, -10, MAX(event_timestamp))
    FROM {{ this }}
  )
  AND event_timestamp IS NOT NULL
{% endif %}

ORDER BY event_timestamp
