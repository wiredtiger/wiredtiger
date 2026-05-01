# test_inmem01 — In-memory configuration smoke tests

**File:** `test/suite/test_inmem01.py`
**Storage mode:** In-memory (`in_memory=true`)
**Components under test:** cache/eviction, in-memory storage, btree

## Test Cases

### `test_inmem01.test_insert`
- **What it tests:** Inserts 1000 rows into an in-memory table and verifies all are visible.
- **Components:** `src/evict/evict_lru.c`, `src/btree/`, `src/conn/conn_stat.c`
- **Notes:** Parameterized by key format: `col` (`key_format='r'`) and `row` (`key_format='S'`). Connection configured with `cache_size=5MB`, `file_manager=(close_idle_time=0)`, `in_memory=true`. Table uses `memory_page_max=32k,leaf_page_max=4k`.

### `test_inmem01.test_insert_over_capacity`
- **What it tests:** Attempts to insert 10 million rows into a 5MB cache, expects `WT_CACHE_FULL` error. Then finds the last successfully inserted key and verifies all prior insertions are intact.
- **Components:** `src/evict/evict_lru.c`, `src/btree/`
- **Notes:** Both `col` and `row` key formats. Validates that partial insertions before cache exhaustion are not corrupted.

### `test_inmem01.test_insert_over_delete`
- **What it tests:** Fills the cache to `WT_CACHE_FULL`, then removes the first 99 rows and confirms removes succeed even with a full cache.
- **Components:** `src/evict/evict_lru.c`, `src/btree/`
- **Notes:** Both key formats. Does not verify re-insert after delete (that is covered by `test_insert_over_delete_replace`).

### `test_inmem01.test_insert_over_delete_replace`
- **What it tests:** Fills cache to `WT_CACHE_FULL`, removes approximately one-quarter of all rows, then retries inserting a single key in a loop (with 1-second sleeps) until eviction reclaims enough space (max 5 minutes). Confirms at least one insert succeeds.
- **Components:** `src/evict/evict_lru.c`, `src/btree/`
- **Notes:** Skipped for `timestamp` hook (`@wttest.skip_for_hook("timestamp", "removing timestamped items will not free space")`). Both key formats.

### `test_inmem01.test_wedge`
- **What it tests:** Repeatedly fills the cache to exhaustion (up to 10 million rows), checks that the cache is "really" full (fewer than 100 new rows fit in a new fill attempt), then verifies all data is readable 100 times with 1-second intervals.
- **Components:** `src/evict/evict_lru.c`, `src/btree/`, `src/cache/`
- **Notes:** Long test (`@wttest.longtest`). Both key formats. Designed to stress the in-memory eviction path to the point of near-saturation.
