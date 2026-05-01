# test_error_info02 — Session get_last_error() for WT_ROLLBACK sub-reasons

**File:** `test/suite/test_error_info02.py`
**Storage mode:** General (disagg hook disabled for cache-overflow test)
**Components under test:** session API (get_last_error), eviction, transaction conflict detection

## Test Cases

### `test_error_info02.test_wt_rollback_cache_overflow`
- **What it tests:** Configures a very low cache (`cache_max_wait_ms=2`, `eviction_dirty_target=1`, `eviction_dirty_trigger=2`), then inserts very large values in a loop until a `WT_ROLLBACK` error occurs due to cache overflow. Asserts `get_last_error()` returns `(WT_ROLLBACK, WT_CACHE_OVERFLOW, "Cache capacity has overflown")`.
- **Components:** `src/evict/`, `src/txn/`, `src/session/`
- **Notes:** Skipped for disagg hook (`FIXME-WT-15058`). The loop runs up to 100 iterations with 5 MB values. Uses `reconfigure` to drop `cache_max_wait_ms` after initial inserts.

### `test_error_info02.test_wt_rollback_write_conflict_update_list`
- **What it tests:** Two sessions both begin transactions and attempt to update the same key; the second session's update should fail with `WT_ROLLBACK`. Asserts `get_last_error()` in the second session returns `(WT_ROLLBACK, WT_WRITE_CONFLICT, "Write conflict between concurrent operations")`. Covers the in-memory update-list conflict path.
- **Components:** `src/txn/`, `src/session/`

### `test_error_info02.test_wt_rollback_write_conflict_time_start`
- **What it tests:** Session 1 begins a transaction; session 2 commits an update; the update is evicted to disk. Session 1 then attempts to insert the same key, which conflicts because the on-disk version's time_start is invisible to session 1's snapshot. Asserts `(WT_ROLLBACK, WT_WRITE_CONFLICT, ...)`.
- **Components:** `src/txn/`, `src/reconcile/`, `src/session/`
- **Notes:** Uses `debug=(release_evict)` cursor to force eviction.

### `test_error_info02.test_wt_rollback_write_conflict_time_stop`
- **What it tests:** Similar to the previous test but the conflict arises from a tombstone (`time_stop`) that is invisible to the first session. Session 2 removes a key that session 1 was about to update; the deletion is evicted; session 1's insert fails. Asserts `(WT_ROLLBACK, WT_WRITE_CONFLICT, ...)`.
- **Components:** `src/txn/`, `src/reconcile/`, `src/session/`

### `test_error_info02.test_wt_rollback_oldest_for_eviction`
- **What it tests:** Configures a 1 MB cache, inserts a 5 MB value in an open transaction to fill the cache, waits 2 seconds, then inserts a second value in the same transaction. Because the first transaction pins the oldest ID, eviction cannot proceed, and the second insert fails with `WT_ROLLBACK`. Asserts `get_last_error()` returns `(WT_ROLLBACK, WT_OLDEST_FOR_EVICTION, "Transaction has the oldest pinned transaction ID")`.
- **Components:** `src/evict/`, `src/txn/`, `src/session/`
