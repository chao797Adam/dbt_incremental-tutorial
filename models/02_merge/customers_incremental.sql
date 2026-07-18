{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_id',
    merge_update_columns=['customer_name', 'email', 'phone', 'country', 'updated_at'],
    file_format='delta',
    partition_by='country',
    cluster_by=['customer_id'],
    on_schema_change='fail'
  )
}}

-- MERGE Strategy on Databricks
-- Delta Lake MERGE is highly optimized

SELECT
    customer_id,
    customer_name,
    email,
    phone,
    country,
    signup_date,
    updated_at,
    CURRENT_TIMESTAMP() as last_processed_at

FROM {{ ref('customers') }}

WHERE customer_name IS NOT NULL
{% if is_incremental() %}
  AND updated_at > (
    SELECT MAX(updated_at)
    FROM {{ this }}
  )
  AND customer_id IS NOT NULL
{% endif %}
