# test_inmem02 — In-memory table with ignore_in_memory_cache_size setting

**File:** `test/suite/test_inmem02.py`
**Storage mode:** In-memory (`in_memory=true`)
**Components under test:** cache/eviction, in-memory storage, table configuration

## Test Cases

### `test_inmem02.test_insert_over_allowed`
- **What it tests:** Verifies that a table created with `ignore_in_memory_cache_size=true` can be written to even after the normal cache is completely full. First fills a normal table to `WT_CACHE_FULL`, then inserts 999 rows into the `ignore_in_memory_cache_size=true` table and confirms they succeed.
- **Components:** `src/evict/evict_lru.c`, `src/btree/`, `src/conn/conn_stat.c`
- **Notes:** Connection: `cache_size=3MB`, `in_memory=true`. The "exempt" table is created before the normal table fills the cache (so the create itself succeeds). The 999-row insert into the exempt table happens after cache saturation, confirming the per-table override works correctly.
