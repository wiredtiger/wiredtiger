# test_prepare08 — Prepared tombstones evicted to data store: rollback and commit

**File:** `test/suite/test_prepare08.py`
**Storage mode:** General
**Components under test:** prepared transactions, tombstones, eviction, history store, visibility

## Test Cases

### `test_prepare08.test_prepare_delete_rollback`
- **What it tests:** Inserts a key, prepares a tombstone (delete) on it using a secondary table to generate eviction pressure, rolls back the prepare; verifies the original value is visible at the appropriate timestamp after rollback
- **Components:** `txn/txn_prepare.c`, `evict/evict_page.c`, `btree/bt_delete.c`
- **Notes:** Uses a large secondary table to trigger eviction of the primary table's pages; verifies that the tombstone from the rolled-back prepare does not persist

### `test_prepare08.test_prepare_update_delete_commit`
- **What it tests:** Inserts a value, then in a prepared transaction performs an update followed by a delete (tombstone); commits the prepare; verifies the key is not found after commit and that prior values are visible at earlier timestamps
- **Components:** `txn/txn_prepare.c`, `evict/evict_page.c`, `btree/bt_delete.c`, `history/hs_cursor.c`
- **Notes:** Tests the case where a prepared transaction contains both an update and a tombstone for the same key (update chain: new_value → tombstone, both prepared); eviction pressure used to move pages to disk

### `test_prepare08.test_prepare_update_delete_commit_with_no_base_update`
- **What it tests:** Prepares a new insert (no prior value) combined with a delete in the same transaction; commits; verifies key is not visible after commit and no ghost tombstone issues occur
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `evict/evict_page.c`
- **Notes:** Covers the edge case where the prepared update chain starts with an insert (no prior disk value) followed by a tombstone; tests that reconciliation correctly handles this pattern
