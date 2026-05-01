# test_eviction05 — Eviction max page size statistics (clean, dirty, updates) and checkpoint reset

**File:** `test/suite/test_eviction05.py`
**Storage mode:** General (skipped for disagg hook)
**Components under test:** eviction, statistics, checkpoint

## Test Cases

### `test_eviction05.test_eviction_page_size_stats`
- **What it tests:** Inserts one key and commits. Evicts the dirty page and asserts `eviction_maximum_updates_page_size_per_checkpoint > 0` and `eviction_maximum_dirty_page_size_per_checkpoint > 0`, while `eviction_maximum_clean_page_size_per_checkpoint == 0`. Then reads the key back (loading a clean page) and evicts it again, asserting that `eviction_maximum_clean_page_size_per_checkpoint > 0`. Finally, runs a checkpoint and asserts all three per-checkpoint max stats reset to zero, confirming they track the maximum within a checkpoint interval.
- **Components:** `src/evict/`, `src/stat/`, `src/session/`
- **Notes:** `conn_config = 'cache_size=10MB,statistics=(all),statistics_log=(json,on_close,wait=1)'`. Skipped for disagg hook. Tests both dirty-page and clean-page eviction paths and verifies the per-checkpoint reset behavior. The clean page read-back uses a separate read cursor before using `debug=(release_evict)` to trigger eviction.
