# test_tiered10 — Simultaneous connections with different prefixes sharing the same bucket

**File:** `test/suite/test_tiered10.py`
**Storage mode:** Tiered
**Components under test:** two concurrent WiredTiger connections in separate home directories sharing one bucket via different bucket prefixes, flush_tier isolation per connection, read-back from bucket after local copy removal

## Test Cases

### `test_tiered10.test_tiered`
- **What it tests:** Two WiredTiger connections (`conn1` in `first_dir`, `conn2` in `second_dir`) are opened simultaneously, each with the same tiered storage bucket but different prefixes (`bucket_prefix` and `bucket_prefix1`). Both create an identically-named table (`test_tiered10`) and insert different data. Each connection independently calls `checkpoint('flush_tier=(enabled,force=true)')`. For dir_store the test verifies that both bucket objects exist with the correct prefixes and distinct content. The local object copies are then removed from each home directory to force reads from the bucket. Both connections are reopened and the data is re-read and verified — each connection reads back its own data correctly, confirming that prefix isolation in the shared bucket prevents cross-connection data corruption.
- **Components:** `src/tiered/conn_tiered.c` (multi-connection bucket sharing), bucket prefix logic in tiered object naming, `local_retention=1`, storage_source (dir_store / s3_store / etc.)
- **Notes:**
  - Parametrized across all tiered storage backends.
  - The standard test connection is opened only as a dummy (to invoke `conn_extensions`); the real test logic uses manually opened `conn1`/`conn2`.
  - For dir_store, the bucket is placed one level up from both home directories (`../bucket`) so both can reference it by relative path.
  - `conn1` inserts key `"0"` and `conn2` inserts key `"20"` to make data unambiguously distinct.
