# test_prepare37 — Eviction of pages with prepared updates: free updates path

**File:** `test/suite/test_prepare37.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, eviction, free updates, checkpoint, commit/rollback

## Test Cases

### `test_prepare37.test_commit_prepare`
- **What it tests:** Multi-step workflow: insert value_a, update to value_b at ts=10, prepare value_c at ts=20, checkpoint (writes prepared update to disk), evict via debug cursor, commit prepared transaction, evict again, verify value_a in HS at ts=5, value_b in HS at ts=15, value_c as current at ts=25
- **Components:** `txn/txn_prepare.c`, `evict/evict_page.c`, `history/hs_cursor.c`, `checkpoint/checkpoint.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`

### `test_prepare37.test_rollback_prepare`
- **What it tests:** Same workflow as test_commit_prepare but rolls back the prepared transaction; verifies value_b is restored as current and value_a is in HS
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `evict/evict_page.c`, `history/hs_cursor.c`
- **Notes:** After rollback, the time window for the prepared update is invalidated; eviction of the same page after rollback verifies cleanup

### `test_prepare37.test_commit_prepare_delete`
- **What it tests:** Same as test_commit_prepare but the prepared transaction performs a delete (tombstone) instead of an update; verifies key is not found after commit and prior values are in HS
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `evict/evict_page.c`, `history/hs_cursor.c`

### `test_prepare37.test_rollback_prepare_delete`
- **What it tests:** Same as test_rollback_prepare but the prepared transaction performs a delete; after rollback, verifies the key is still visible with its last committed value
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `btree/bt_delete.c`, `evict/evict_page.c`
