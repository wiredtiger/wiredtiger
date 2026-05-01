# test_app_thread_evict01 — Application thread pulled into eviction when trigger levels exceeded

**File:** `test/suite/test_app_thread_evict01.py`
**Storage mode:** General
**Components under test:** eviction, cache management, statistics

## Test Cases

### `test_app_thread_evict01.test_app_thread_evict01`
- **What it tests:** Verifies that when a write causes the cache to exceed eviction trigger levels (52% dirty/updates), an application thread is pulled into eviction and the `application_evict_snapshot_refreshed` statistic is incremented. Inserts 40 MB of data (below trigger), then inserts two 20 MB records in a single transaction to push the cache over the trigger threshold, which should cause the second insert to trigger app-thread eviction.
- **Components:** `src/evict/evict_lru.c`, `src/evict/evict_page.c`, `src/conn/conn_stat.c`
- **Notes:** 100 MB cache with 1 eviction thread; trigger=52%, target=50% for all three eviction categories (dirty, updates, total). Test is probabilistic — the application thread races the single internal eviction thread; up to 20 attempts are made. Only `key_format=i, value_format=S` scenario is currently defined.
