# test_stat12 — Eviction trigger and cache fill-ratio statistics

**File:** `test/suite/test_stat12.py`
**Storage mode:** General
**Components under test:** eviction trigger statistics, cache fill ratio statistics

## Test Cases

### `test_stat12.test_stats_eviction_trigger_exist`
- **What it tests:** Checks that `cache_eviction_trigger_reached`, `cache_eviction_trigger_dirty_reached`, and `cache_eviction_trigger_updates_reached` statistics exist and return non-None values.
- **Components:** `stat.c`, `evict.c`
- **Notes:** Pure existence check; no data population.

### `test_stat12.test_stats_eviction_fill_ratio_exist`
- **What it tests:** Checks that `cache_eviction_app_threads_fill_ratio_lt_25`, `_25_50`, `_50_75`, `_gt_75` statistics exist and return non-None values.
- **Components:** `stat.c`, `evict.c`
- **Notes:** Pure existence check; no data population.

### `test_stat12.test_stats_eviction_trigger_increments`
- **What it tests:** Populates 5000 records (2000-byte values) to fill the 1 MB cache; does dirty writes and clean reads; waits up to 20 seconds for at least one eviction trigger stat and one fill-ratio stat to become non-zero; then asserts `eviction_trigger_count >= 1` and `fill_ratio_count >= 1`; also asserts `eviction_trigger_count >= fill_ratio_count`.
- **Components:** `stat.c`, `evict.c`, `cache.c`
- **Notes:** Small cache (1 MB) forces eviction. Multiple trigger types (clean, dirty, updates) can fire simultaneously while fill-ratio is counted once per application-thread eviction pass, so trigger count >= fill-ratio count.
