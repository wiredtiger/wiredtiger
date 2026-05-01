# test_cursor17 — cursor.largest_key() across MVCC scenarios

**File:** `test/suite/test_cursor17.py`
**Storage mode:** General
**Components under test:** cursor largest_key API, MVCC, prepared transactions, truncate, timestamps

## Test Cases

### `test_cursor17.test_globally_deleted_key`
- **What it tests:** `largest_key()` on a table where the largest key has been globally deleted; verifies the deleted key is still returned (largest_key ignores visibility).
- **Components:** `src/cursor/cur_std.c`, `src/btree/`
- **Notes:** Skipped for timestamp hook.

### `test_cursor17.test_uncommitted_insert`
- **What it tests:** `largest_key()` when the largest key was inserted in an uncommitted transaction; verifies the uncommitted key is returned.
- **Components:** `src/cursor/cur_std.c`, `src/txn/`

### `test_cursor17.test_aborted_insert`
- **What it tests:** `largest_key()` after the largest key's insert was aborted; verifies the key is no longer returned.
- **Components:** `src/cursor/cur_std.c`, `src/txn/`

### `test_cursor17.test_invisible_timestamp`
- **What it tests:** `largest_key()` when the largest key has a timestamp not yet visible; verifies it is still returned (largest_key is timestamp-agnostic).
- **Components:** `src/cursor/cur_std.c`, `src/txn/txn_timestamp.c`

### `test_cursor17.test_prepared_update`
- **What it tests:** `largest_key()` when the largest key has a prepared (uncommitted) update; verifies it is returned.
- **Components:** `src/cursor/cur_std.c`, `src/txn/`

### `test_cursor17.test_not_positioned`
- **What it tests:** `largest_key()` on an unpositioned cursor; verifies correct behavior (no error, returns largest key).
- **Components:** `src/cursor/cur_std.c`

### `test_cursor17.test_get_value`
- **What it tests:** `largest_key()` followed by `get_value()` — verifies that after `largest_key()` the value is not accessible (key-only).
- **Components:** `src/cursor/cur_std.c`

### `test_cursor17.test_empty_table`
- **What it tests:** `largest_key()` on an empty table; expects `WT_NOTFOUND`.
- **Components:** `src/cursor/cur_std.c`

### `test_cursor17.test_fast_truncate`
- **What it tests:** `largest_key()` after a fast truncate of the entire table; verifies the truncated key is or is not returned depending on implementation.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_delete.c`
- **Notes:** Skipped for timestamp hook.

### `test_cursor17.test_slow_truncate`
- **What it tests:** `largest_key()` after a slow (key-by-key) truncate; verifies behavior.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_delete.c`
- **Notes:** Skipped for timestamp hook. Scenarios: file-row, table-row, file-var, table-var, table-r-complex.
