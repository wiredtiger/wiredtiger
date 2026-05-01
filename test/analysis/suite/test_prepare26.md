# test_prepare26 — Prepare rollback with key delete, oldest advance, and eviction

**File:** `test/suite/test_prepare26.py`
**Storage mode:** General
**Components under test:** prepared transactions, rollback, tombstones, oldest_timestamp, eviction, visibility

## Test Cases

### `test_prepare26.test_prepare26`
- **What it tests:** Inserts a value, prepares an update, rolls back the prepare; then deletes the key at a new timestamp; advances `oldest_timestamp` past the delete; evicts the page; inserts more updates on other keys; verifies that no visible record exists at oldest_timestamp (the deleted key is globally invisible)
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `btree/bt_delete.c`, `evict/evict_page.c`, `txn/txn_timestamp.c`
- **Notes:** No scenarios; the sequence (rollback → delete → advance oldest → evict) tests that the tombstone from the post-prepare delete is correctly made globally visible when oldest advances past it, and that reconciliation drops the key entirely from the page; verifies no spurious reads after eviction
