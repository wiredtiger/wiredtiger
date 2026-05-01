# test_prepare41 — Update restore eviction with prepared modifies and prepared deletes

**File:** `test/suite/test_prepare41.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, modify, delete, update restore eviction, history store, checkpoint

## Test Cases

### `test_prepare41.test_prepare_update`
- **What it tests:** Tests update restore eviction for a prepared `wiredtiger.Modify` operation: inserts base value, prepares a modify, rolls back the prepare, evicts (triggering update restore), checkpoints; verifies that the HS correctly contains the pre-modify value and the modify's delta is not present after rollback
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `modify/modify.c`, `evict/evict_page.c`, `history/hs_cursor.c`, `checkpoint/checkpoint.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; no scenarios; update restore eviction occurs when a page cannot be cleanly evicted due to uncommitted or prepared updates — the page is rewritten but the update list is restored in memory

### `test_prepare41.test_prepare_delete`
- **What it tests:** Same as test_prepare_update but uses a prepared delete (tombstone) instead of a modify; after rollback and eviction, verifies the HS has the correct value restored and the key is still visible
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `btree/bt_delete.c`, `evict/evict_page.c`, `history/hs_cursor.c`
- **Notes:** Companion to test_prepare_update; exercises the delete path through update restore eviction
