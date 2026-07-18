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

-- INSERT_OVERWRITE Strategy: Databricks Native
-- Highly efficient for partitioned Delta tables

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
  WHERE event_date >= (
    SELECT COALESCE(MAX(event_date), '1900-01-01') FROM {{ this }}
  ) - INTERVAL 1 DAY
{% endif %}

ORDER BY event_timestamp
