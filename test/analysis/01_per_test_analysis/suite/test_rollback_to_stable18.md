# test_rollback_to_stable18 — RTS does not skip pages lacking aggregated time window

**File:** `test/suite/test_rollback_to_stable18.py`
**Storage mode:** General (in-memory only)
**Components under test:** rollback_to_stable, aggregated time windows, in-memory, eviction

## Test Cases

### `test_rollback_to_stable18.test_rollback_to_stable`
- **What it tests:** Verifies that RTS does not incorrectly skip pages that lack an aggregated time window (i.e., pages that were evicted and then re-read without their time window being repopulated). Writes 10,000 rows at ts=20 (value_a), then removes them at ts=30. Sets stable=20 initially. After writes, evicts page 1 via `debug=(release_evict)`. Then advances stable to ts=20 (non-prepare) or ts=30 (prepare). Calls RTS. Verifies value_a is visible at ts=30 (removes were rolled back). Stats: `calls=1`, `upd_aborted == nrows`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/btree/`, `src/evict/`
- **Notes:** Always in-memory mode (`in_memory=true` in conn_config, no disk option). Parametrized on key_format (column/row_integer), prepare, worker threads (0/4/8). Uses `eviction_dirty_trigger=10,eviction_updates_trigger=10` to trigger eviction under pressure. Tagged `[TEST_TAGS] rollback_to_stable, aggregated_time_windows`.
