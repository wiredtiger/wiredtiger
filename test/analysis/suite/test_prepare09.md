# test_prepare09 — Rollback of prepared update does not incorrectly insert tombstone

**File:** `test/suite/test_prepare09.py`
**Storage mode:** General
**Components under test:** prepared transactions, rollback, tombstones, visibility

## Test Cases

### `test_prepare09.test_prepared_update_is_aborted_correctly_with_on_disk_value`
- **What it tests:** Inserts a value at timestamp 10, evicts the page to disk, then prepares an update; rolls back the prepared update; verifies the original value is still visible (no spurious tombstone was inserted) and the prepared update is gone
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `btree/bt_cursor.c`, `evict/evict_page.c`
- **Notes:** The on-disk value is present before prepare; the bug this test guards against is that rollback might incorrectly place a tombstone rather than simply discarding the prepared update; verifies value at ts=10 is readable after rollback

### `test_prepare09.test_prepared_update_is_aborted_correctly`
- **What it tests:** Same as above but without first evicting the page; the prepared update exists only in memory when rolled back; verifies no spurious tombstone and original value visible
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `btree/bt_cursor.c`
- **Notes:** Covers the in-memory (non-evicted) path for rollback; companion test to the on-disk variant
