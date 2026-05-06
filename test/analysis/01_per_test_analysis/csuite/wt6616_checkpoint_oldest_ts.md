# wt6616_checkpoint_oldest_ts — Checkpoint oldest-timestamp visibility after crash recovery

**Path:** `test/csuite/wt6616_checkpoint_oldest_ts/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-6616
**Components under test:** Checkpoint with timestamps (`use_timestamp=true`), oldest timestamp, stable timestamp, crash recovery (SIGKILL child), timestamp-based read visibility

## What This Test Does
This test verifies that after a crash during a workload that continuously inserts and immediately deletes records under a monotonically advancing stable timestamp, all records from the oldest to stable timestamp of the last successful checkpoint are visible and correct after recovery. A child process inserts key X at timestamp X, advances the stable timestamp to X, deletes the key at timestamp X+1, and periodically advances the oldest timestamp to make half the data obsolete. The parent kills the child after a random delay (10–40 seconds), reopens the database to trigger log recovery, and queries the post-recovery stable and oldest timestamps, then reads every key in that range using the matching read timestamp and verifies each is found.

## Test Scenarios / Cases

### Scenario: Row-store insert/delete cycle with checkpoint_slow timing stress, crash, and recovery
- **What it tests:** That all keys from oldest_timestamp to stable_timestamp (of the last checkpoint) are visible at their corresponding read timestamps after crash recovery.
- **Components:** `session->checkpoint(use_timestamp=true)`, `conn->set_timestamp(stable/oldest)`, `conn->query_timestamp`, fork/SIGKILL, log recovery, `timing_stress_for_test=[checkpoint_slow]`.
- **Notes:** MAX_DATA=1000 (half-life window), MAX_TIME=40s, MIN_TIME=10s. Table uses `log=(enabled=false)` (non-logged). Sentinel file `checkpoint_done` used to synchronize parent kill timing.

### Scenario: Column-store variant
- **What it tests:** Same crash-recovery visibility check using a column-store table (`key_format=r`), activated with the `-c` flag.
- **Components:** Column-store, timestamp checkpoint, SIGKILL recovery.

## LazyFS Variant
None.
