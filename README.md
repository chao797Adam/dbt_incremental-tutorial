# dbt + Databricks Incremental Strategies Practice

A hands-on practice project exploring dbt-databricks incremental model strategies, built while following a public tutorial. This repo diverges from the original tutorial in several places where I ran into real issues on my own dataset and fixed them — noted below.

## Project structure

- `seeds/` — raw CSV data (events, customers, products, sales_detail)
- `models/01_append` ~ `05_microbatch` — one folder per incremental strategy
- Data flow: seed → bronze/raw → silver

## Strategies covered

| Strategy | What it does | Supported on dbt-databricks |
|---|---|---|
| append | Pure insert, no dedup | Yes |
| merge | Upsert by `unique_key` | Yes |
| delete+insert | Delete matching rows, then insert | Yes (requires a recent dbt-databricks version) |
| insert_overwrite | Overwrites whole partitions | Yes |
| microbatch | Automatic time-based batching (dbt 1.9+) | Yes (uses `replace_where` under the hood) |

## Differences from the original tutorial

The tutorial was recorded against a moving "today," so several places relied on `CURRENT_DATE()` as the incremental filter boundary. Since my seed data is a fixed historical dataset (Jan 2026), `CURRENT_DATE()` never overlaps with it, so I made the following changes:

1. **Incremental filter boundary (all strategies)** — the tutorial uses `CURRENT_DATE() - INTERVAL N DAYS` as the lower bound. I replaced this with a filter based on the max value already in the target table:
   ```sql
   WHERE event_date >= (SELECT COALESCE(MAX(event_date), '1900-01-01') FROM {{ this }}) - INTERVAL N DAYS
   ```
   This makes the incremental logic self-contained and independent of wall-clock time, so it works correctly on a static/historical dataset instead of silently no-op'ing.

2. **`03_delete_insert` — deduplication before aggregation** — I found that duplicate `sale_id` rows in the source data (simulating an updated record) caused `COUNT(DISTINCT sale_id)` and `SUM(total_amount)` to disagree: `COUNT` deduped correctly but `SUM` summed every physical row, double-counting the old and new values. I added a dedup CTE before aggregating:
   ```sql
   ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY updated_at DESC) as rn
   ```
   keeping only `rn = 1`, so every business key contributes exactly once, using its latest known value. Note: this is only correct when duplicate rows represent the same fact being updated — I intentionally did *not* apply this pattern to cases where a duplicate key turned out to be an unrelated ID collision (different user/event/amount), since blindly deduping there would silently discard real data.

3. **No `post_hook` optimization step** — the tutorial's `01_append` example includes `post_hook` calls to `OPTIMIZE` and `ANALYZE TABLE COMPUTE STATISTICS`. I left these out for now — they're Delta Lake file-compaction and query-optimizer statistics tuning, which only matter at real data volumes. On a few dozen rows there's nothing to observe, so I skipped it rather than copy code I couldn't verify. Noted as a follow-up below.

4. **`05_microbatch` — `end` is not a model config** — the tutorial's config includes `end='...'` inside `config()`. This silently does nothing: dbt-databricks has no `end` config key for microbatch, so without it the batch range defaults to "now," which on a historical dataset tries to generate ~170 daily batches (from the seed's start date to today). I removed `end` from the config and instead scoped the batch range at run time with CLI flags:
   ```bash
   dbt run --select 05_microbatch --full-refresh --event-time-start "2026-01-30" --event-time-end "2026-02-01"
   ```
   This correctly produces exactly 2 batches matching the 2 days present in the seed data.

## Environment

- dbt-core / dbt-databricks
- Databricks SQL Warehouse

## Follow-up / not yet explored

- `post_hook` performance tuning (`OPTIMIZE`, `ANALYZE TABLE COMPUTE STATISTICS`) — meaningful only at production data volumes, revisit later.
