{{
  config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_timestamp',
    batch_size='day',
    lookback=0,
    begin='2026-01-30',
    file_format='delta',
    on_schema_change='fail'
  )
}}

/*
╔═══════════════════════════════════════════════════════════════════════════╗
║                   SIMPLE MICROBATCH DEMO                                  ║
║                                                                           ║
║  This demonstrates the MICROBATCH strategy (dbt 1.9+)                   ║
║  Perfect for: Streaming data, late-arriving data, automatic batching    ║
╚═══════════════════════════════════════════════════════════════════════════╝

🎯 **WHAT IS MICROBATCH?**
Microbatch is dbt's smartest incremental strategy. Instead of processing ALL
new data at once, it divides the work into time-based batches and processes 
them one at a time.

💡 **KEY BENEFITS:**

1. **Automatic Date Filtering**
   - You don't write WHERE event_date >= ... logic
   - dbt automatically filters each batch for you
   - Just set event_time='event_timestamp' and batch_size='day'

2. **Late Arrival Handling**
   - lookback=0: Process only new batches
   - lookback=2: Reprocess last 2 days + new data
   - Perfect for data that arrives late!

3. **Better Performance**
   - Smaller batches = faster queries
   - Failed batch? Only retry that batch, not everything
   - Easier to debug and monitor

4. **Simpler Code**
   - No manual is_incremental() logic needed
   - No complex date range calculations
   - dbt handles all the complexity

📅 **HOW IT WORKS:**

Initial run (--full-refresh):
  Step 1: dbt sees begin='2026-01-30' and end='2026-01-30'  
  Step 2: Creates batch for 2026-01-30
  Step 3: Processes only events from that day
  Step 4: Table created with 10 events!

Future incremental runs:
  Step 1: dbt checks last batch processed (2026-01-30)
  Step 2: Sees today is 2026-01-31
  Step 3: Creates batch for 2026-01-31
  Step 4: Processes only new day's events
  Step 5: Inserts 10 more events!

With lookback=2:
  → Reprocesses 2026-01-29 and 2026-01-30
  → Then processes new 2026-01-31 data
  → Catches any late-arriving corrections!

⚡ **VS OTHER STRATEGIES:**

APPEND:          ← Just dumps all new rows (no updates, no dedup)
MERGE:           ← Updates + inserts (SLOW on big tables)
DELETE+INSERT:   ← Manual partition logic (you write the dates)
INSERT_OVERWRITE:← Replaces partitions (need is_incremental check)
MICROBATCH:      ← 🏆 Automatic, smart, handles late data!

*/

SELECT
    event_id,
    user_id,
    event_name,
    event_timestamp,        -- 🎯 THIS is what dbt uses for batching!
    product_id,
    amount,
    event_date,
    
    -- Show which batch this came from
    DATE_TRUNC('day', event_timestamp) as batch_day,
    
    -- When was this processed by dbt?
    CURRENT_TIMESTAMP() as processed_at,
    '{{ run_started_at }}' as dbt_run_time

FROM {{ source('raw_data', 'events') }}

WHERE 
    event_timestamp IS NOT NULL

-- ✨ dbt automatically adds its own WHERE clause here:
--    WHERE event_timestamp >= '2026-01-30 00:00:00'
--      AND event_timestamp < '2026-01-31 00:00:00'  (for batch 1)
--
--    WHERE event_timestamp >= '2026-01-31 00:00:00'
--      AND event_timestamp < '2026-02-01 00:00:00'  (for batch 2)
--
-- You don't write it - dbt does it for you based on:
--   • event_time='event_timestamp'
--   • batch_size='day'  
--   • Current batch being processed
