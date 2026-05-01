# test_eviction04 — In-memory restoration due to invisible updates during eviction

**File:** `test/suite/test_eviction04.py`
**Storage mode:** General (skipped for disagg hook)
**Components under test:** eviction, reconciliation (in-memory restore), statistics

## Test Cases

### `test_eviction04.test_eviction`
- **What it tests:** Inserts key 1 (committed) and key 2 (in an open transaction, not yet committed) via a second session. Forces eviction of key 1 using a `debug=(release_evict)` cursor. Asserts that `cache_write_restore_invisible` > 0, confirming that reconciliation performed an in-memory restore because key 2's update was invisible (the second session's transaction hadn't committed yet).
- **Components:** `src/evict/`, `src/reconcile/`
- **Notes:** `conn_config = 'cache_size=10MB,statistics=(all),statistics_log=(json,on_close,wait=1)'`. Skipped for disagg hook. After asserting the stat, commits session 2's transaction to allow clean teardown.

### Eviction trigger
- Manual via `debug=(release_evict)` cursor reset on key 1. The presence of an uncommitted update (key 2) in the same btree forces reconciliation to do an in-memory restore. Correctness property: `cache_write_restore_invisible` must be incremented.
