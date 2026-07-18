{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by='event_date',
    file_format='delta',
    cluster_by=['user_id', 'event_name'],
    on_schema_change='fail'
  )
}}

-- INSERT_OVERWRITE with Advanced Delta Lake Features

SELECT
    event_date,
    user_id,
    event_name,
    COUNT(*) as event_count,
    SUM(amount) as total_amount,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event,
    CURRENT_TIMESTAMP() as processed_at

FROM {{ ref('events') }}

{% if is_incremental() %}
  WHERE event_date >= (
    SELECT COALESCE(MAX(event_date), '1900-01-01') FROM {{ this }}
  ) - INTERVAL 1 DAY
{% endif %}

GROUP BY event_date, user_id, event_name

ORDER BY event_date DESC, total_amount DESC
