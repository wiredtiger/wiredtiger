# test_stat15 — Cache pages in-use and leaf pages statistics

**File:** `test/suite/test_stat15.py`
**Storage mode:** General
**Components under test:** `cache_pages_inuse`, `cache_pages_inuse_leaf`

## Test Cases

### `test_stat15.test_cache_pages_inuse_leaf`
- **What it tests:** Inserts 1000 records, then verifies `cache_pages_inuse_leaf > 0` and `cache_pages_inuse >= cache_pages_inuse_leaf`.
- **Components:** `stat.c`, `cache.c`, `btree`
- **Notes:** Confirms leaf page tracking is distinct from total page tracking.

### `test_stat15.test_cache_pages_inuse_leaf_decreases_after_eviction`
- **What it tests:** Inserts 10,000 records (1 KB values) to populate multiple leaf pages; checkpoints; reads the leaf page count; reopens the connection (clearing the cache); then verifies that `cache_pages_inuse_leaf` after reopen is less than before.
- **Components:** `stat.c`, `cache.c`, `btree`
- **Notes:** Uses `reopen_conn()` to trigger cache eviction.
