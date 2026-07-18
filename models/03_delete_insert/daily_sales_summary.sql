{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='sale_date',
    file_format='delta',
    partition_by='sale_date',
    on_schema_change='fail'
  )
}}

-- DELETE+INSERT Strategy on Databricks
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
),

daily_aggregation AS (
  SELECT
      sale_date,
      COUNT(DISTINCT customer_id) as num_customers,
      COUNT(DISTINCT sale_id) as num_transactions,
      SUM(total_amount) as total_revenue,
      AVG(total_amount) as avg_transaction_value,
      MIN(total_amount) as min_transaction,
      MAX(total_amount) as max_transaction,
      SUM(quantity) as total_units_sold,
      MAX(updated_at) as last_updated

  FROM latest_sales
  GROUP BY sale_date
)

SELECT
    sale_date,
    num_customers,
    num_transactions,
    total_revenue,
    avg_transaction_value,
    min_transaction,
    max_transaction,
    total_units_sold,
    last_updated,
    CURRENT_TIMESTAMP() as calculated_at

FROM daily_aggregation
WHERE num_transactions > 0
ORDER BY sale_date DESC