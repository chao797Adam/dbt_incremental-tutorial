{{
  config(
    materialized = 'table',
    file_format = 'delta',
    )
}}

select 
event_id,
user_id,
event_name,
event_timestamp,
product_id,
amount,
event_date,
current_timestamp() as processed_at
from {{ ref('events') }}