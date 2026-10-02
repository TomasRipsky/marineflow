# Plan: replayed batches leave duplicate positions

Status: **open, not started**. Found at the end of the first long end-to-end run (4 h 25 min, 1.26 million positions). Small (0.08%), understood, and fixable in two cheap steps.

## What was found

| Layer | Rows | Distinct `(MMSI, time)` | Duplicates |
|---|---|---|---|
| Kafka `vessel-positions` (the input) | 1,263,864 | — | — |
| Bronze (GCS and `vessel-positions-bronze`) | 1,264,863 | 1,263,864 | **999** |
| Silver | 1,264,819 | 1,263,838 | **981** |

Two observations make the cause clear:

1. The distinct count in Bronze equals the input count **exactly**: nothing was lost, and nothing that was not in the input was invented. The only anomaly is the 999 extra rows.
2. They come from one Bronze batch, written twice: `b69fa8d8` at 13:30:10 UTC and again as `d1401f47` at 13:56:00 UTC (999 rows each, same messages). Silver then processed that pair too (`b69fa8d8` and `d1401f47`, 981 rows each) at 13:57. The second copy appeared right after the Bronze job was restarted at 13:55.

How to measure it again (read-only):

```sql
-- duplicates in Silver
SELECT COUNT(*) - COUNT(DISTINCT CONCAT(mmsi, '|', CAST(event_timestamp AS STRING))) AS duplicate_rows
FROM `marineflow-489815.marineflow_silver.vessel_positions_clean`;

-- the batches involved: every batch that holds a duplicated (mmsi, event_timestamp)
WITH dup AS (
  SELECT mmsi, event_timestamp FROM `marineflow-489815.marineflow_silver.vessel_positions_clean`
  GROUP BY 1, 2 HAVING COUNT(*) > 1)
SELECT s._bronze_batch_id, COUNT(*) AS rows_in_duplicated_groups, MIN(s.processing_timestamp) AS processed_at
FROM `marineflow-489815.marineflow_silver.vessel_positions_clean` s JOIN dup USING (mmsi, event_timestamp)
GROUP BY 1 ORDER BY 2 DESC;
```

## Why it happens

Spark Structured Streaming gives **at-least-once** delivery to these sinks, not exactly-once. Each micro-batch does `foreachBatch`: write Parquet to GCS, write to Kafka, and only then does Spark save the batch's offsets in the checkpoint. If the process dies between the writes and the checkpoint, the restart finds the checkpoint one batch behind and **replays that batch**. The replay is a new batch with a new random `batch_id` (the jobs use a UUID), so nothing marks it as the same data, and the Kafka sink cannot reject it.

It is rare: it needs a kill at exactly the wrong moment. The supervised restarts of `scripts/jobs.sh` make a kill survivable, but they do not make it exactly-once.

## Plan, in order

**Step 1: deduplicate where the data is read (small, safe, do this first).**
In `stg_vessel_positions`, keep one row per `(mmsi, event_timestamp)`:

```sql
qualify row_number() over (partition by mmsi, event_timestamp order by processing_timestamp desc) = 1
```

- Every gold model reads the staging view, so one change removes the duplicates from all of them.
- Add a dbt test: `unique` on `(mmsi, event_timestamp)` for the staging view.
- Cost: one window over the table in BigQuery per query. Measure it on 1.3 million rows before and after; if it matters, move the deduplication into a table built by the DAG.
- Acceptance: the Silver query above can still report duplicates (the files are untouched), but gold has none and the new test passes.

**Step 2: make the writes idempotent (the real fix, do after step 1).**
`foreachBatch` receives a `batch_id` that is **stable across a replay**; the jobs ignore it and mint a UUID. Use it:

- name the output location after it (`.../batch=<batch_id>/`) and write with overwrite for that path only, so a replay replaces its own files instead of adding new ones;
- keep the Kafka write as it is (it cannot be made idempotent), and rely on the deduplication of step 1 for consumers;
- add a smoke test on real Spark that replays the same batch twice and asserts the output has no duplicates (the existing `tests/smoke/` pattern: a fake would hide exactly this).
- Check `compact_silver.py` still works with the new layout.

**Step 3 (optional): the hot path.** `hot_alerts.py` reads the Bronze topic, so a duplicate position can raise a duplicate alert. Its state is keyed per vessel; a repeated `(mmsi, time)` should be ignored. Cheap to add once step 1 exists.

## Out of scope

Exactly-once end to end (a transactional Kafka sink, or Delta/Iceberg tables with merge) is the textbook answer and a much bigger change. Steps 1 and 2 reach "no duplicates in the analytics" without it.

## Evidence kept

Counts and batch ids above come from the run of 2026-10-02. The incident is also listed under *Known limitations* in the README.
