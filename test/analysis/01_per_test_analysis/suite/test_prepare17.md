# test_prepare17 — Cache stuck does not occur during prepared transaction commit under eviction pressure

**File:** `test/suite/test_prepare17.py`
**Storage mode:** General
**Components under test:** prepared transactions, eviction pressure, cache management

## Test Cases

### `test_prepare17.test_prepare_cache_stuck_trigger`
- **What it tests:** Verifies that committing a prepared transaction does not trigger a "cache stuck" condition when eviction triggers have been exceeded; uses a 1 MB cache with eviction trigger at 39% and inserts a ~400 KB prepared update to push the cache above the trigger threshold
- **Components:** `txn/txn_prepare.c`, `evict/evict_lru.c`, `conn/conn_cache.c`
- **Notes:** No scenarios; conn_config uses `cache_size=1MB,eviction_updates_trigger=39`; the test guards against a regression where the eviction subsystem could incorrectly report "cache stuck" (which would abort the connection) when eviction was running during a prepare commit; verifies that commit succeeds without error and the correct value is visible afterward
