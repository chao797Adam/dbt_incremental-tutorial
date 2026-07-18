{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['sale_date', 'customer_id'],
    file_format='delta',
    partition_by='sale_date',
    cluster_by=['customer_id'],
    on_schema_change='fail'
  )
}}

-- DELETE+INSERT with Composite Key
-- Dedup by sale_id first (keep latest by updated_at), then aggregate

WITH deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY sale_id
            ORDER BY updated_at DESC
        ) as rn
    FROM {{ ref('sales_detail') }}
    {% if is_incremental() %}
      WHERE sale_date >= CURRENT_DATE() - INTERVAL 3 DAYS
         OR sale_date < '2026-02-01'
    {% endif %}
),

latest_sales AS (
    SELECT * FROM deduplicated WHERE rn = 1
)

SELECT
    sale_date,
    customer_id,
    COUNT(DISTINCT sale_id) as num_purchases,
    SUM(total_amount) as daily_spend,
    AVG(total_amount) as avg_order_value,
    SUM(quantity) as units_purchased,
    MAX(updated_at) as last_transaction_update

FROM latest_sales
GROUP BY sale_date, customer_id
ORDER BY sale_date DESC, daily_spend DESC