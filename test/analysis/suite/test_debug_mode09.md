# test_debug_mode09 — Tests debug_mode update_restore_evict forces update-restore eviction path

**File:** `test/suite/test_debug_mode09.py`
**Storage mode:** General
**Components under test:** debug mode, eviction, cache pressure, update-restore eviction

## Test Cases

### `test_debug_mode09.test_update_restore_evict`
- **What it tests:** Verifies that with `debug_mode=(update_restore_evict=true)` combined with a small cache (10 MB) and a low eviction target (10%), the update-restore eviction path is actually exercised. Inserts 20,000 rows of 500 bytes each to saturate the cache, then checks that the `cache_write_restore_scrub` statistic counter is greater than zero.
- **Components:** `src/evict/`, `src/conn/conn_debug.c`, `src/support/stat.c`
- **Notes:** Uses `statistics=(all)`. The update-restore eviction path writes a page back to disk while preserving in-memory updates for visibility. The small cache and low eviction target are needed because the flag is only effective under cache pressure.
