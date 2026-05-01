# test_hs_evict_race01 — Race: non-timestamped update + eviction mid-checkpoint produces consistent recovery

**File:** `test/suite/test_hs_evict_race01.py`
**Storage mode:** General
**Components under test:** history store, eviction, checkpoint, non-timestamped updates, crash recovery

## Test Cases

### `test_hs_evict_race01.test_mm_ts`
- **What it tests:** Reproduces a race condition (missed-timestamp fix) where:
  1. Insert value1 at ts=4, stable=4. Insert value2 at ts=6.
  2. Start a background thread (`no_timestamp_update_and_evict`) that: sleeps 0.5 s, inserts value4 without a timestamp, sleeps 1.5 s, then evicts the key using a `debug=(release_evict)` cursor.
  3. The main thread calls `session.checkpoint()` (which is slowed by `checkpoint_slow` timing stress).
  4. The checkpoint starts between the OOO insert and the eviction, creating the race: an OOO update gets inserted after the checkpoint takes its snapshot but before it processes the btree, then eviction fails it due to missing-timestamp handling with a flag incorrectly set on the update.
  5. After the thread joins, simulates a crash and restarts.
  6. Reads at ts=4 and asserts the recovered value is value1 (not corrupted).
  
  Without the related fix, the checkpoint was inconsistent: an update flag was incorrectly set, causing the wrong value to survive crash recovery.
- **Components:** `src/history/`, `src/evict/`, `src/checkpoint/`, `src/txn/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`}; `conn_config = 'timing_stress_for_test=(checkpoint_slow)'`. Uses `simulate_crash_restart` helper. Background thread sleeps are carefully timed: 0.5 s before OOO insert (to let checkpoint start), 1.5 s before eviction (to let checkpoint take its snapshot and begin btree processing). numrows=1.
