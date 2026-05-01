# test_prepare12 — Update restore eviction with prepared transaction in cache

**File:** `test/suite/test_prepare12.py`
**Storage mode:** General
**Components under test:** prepared transactions, update restore eviction, eviction, cache

## Test Cases

### `test_prepare12.test_prepare_update_restore`
- **What it tests:** Triggers update restore eviction while a prepared transaction is open in the cache; verifies that the eviction path correctly handles the prepared update and that the data is readable after eviction completes
- **Components:** `txn/txn_prepare.c`, `evict/evict_page.c`, `btree/bt_rec.c`
- **Notes:** No scenarios; uses a small cache to force eviction; an uncommitted (non-prepared) key is also present on the same page to trigger the update restore path; another committed update set drives eviction via cache pressure; guards against assertion failures or data corruption when update restore eviction encounters a prepared update; verifies values at the correct timestamps after eviction
