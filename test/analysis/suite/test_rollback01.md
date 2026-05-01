# test_rollback01 — WT_ROLLBACK on cursor->next() under cache pressure with no auto-retry

**File:** `test/suite/test_rollback01.py`
**Storage mode:** General
**Components under test:** cache eviction, cursor, transactions, rollback

## Test Cases

### `test_rollback.test_wt_rollback_cursor_next_no_retry`
- **What it tests:** Verifies that when a thread is pulled into eviction while calling `cursor->next()` and receives `WT_ROLLBACK`, no automatic retry is performed. After rollback, the cursor must be unpositioned (get_key fails with `requires key be set`). Trigger is reconfiguring `cache_max_wait_ms=2,cache_size=1MB` then inserting a 5 MB value to overflow cache; a positioned read cursor in a second session then calls `next()` up to 80 times until rollback occurs.
- **Components:** `src/evict/`, `src/cursor/`, `src/txn/`
- **Notes:** Skipped for disagg hook (`@wttest.skip_for_hook("disagg", ...)`). Uses `debug=(release_evict)` cursor to force pages to disk first. Expects `WT_ROLLBACK` to be raised within 80 `next()` calls and `get_key` to fail afterward. Ignores `"Cache capacity has overflown"` stdout.
