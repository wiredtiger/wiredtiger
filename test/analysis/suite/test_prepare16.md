# test_prepare16 — Per-key-page eviction with prepared transaction: commit and rollback

**File:** `test/suite/test_prepare16.py`
**Storage mode:** General and in-memory
**Components under test:** prepared transactions, eviction, debug cursor, visibility

## Test Cases

### `test_prepare16.test_prepare`
- **What it tests:** Creates a table where each of 1,000 keys resides on its own leaf page (via very small `allocation_size=512B,leaf_page_max=512B`); inserts an initial value, prepares an update, evicts each page via debug cursor, then commits or rolls back; verifies correct value visibility at the relevant timestamps
- **Components:** `txn/txn_prepare.c`, `evict/evict_page.c`, `btree/bt_rec.c`
- **Notes:** Scenarios: no_inmem/inmem × column/integer-row × commit/rollback; the tiny page sizes ensure every key has its own page, maximizing the number of reconciliation cycles; after commit, value at ts=20 should be the prepared value; after rollback, value at ts=20 should be the original; in-memory variant skips the eviction step
