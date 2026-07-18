{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_sk',
    file_format='delta',
    partition_by='country',
    cluster_by=['customer_sk'],
    on_schema_change='fail'
  )
}}

-- MERGE with Surrogate Key (Best Practice)
-- Works reliably with composite natural keys

SELECT
    -- Generate surrogate key
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
    
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
    SELECT COALESCE(MAX(updated_at), TIMESTAMP '1970-01-01')
    FROM {{ this }}
  )
  AND customer_id IS NOT NULL
{% endif %}
