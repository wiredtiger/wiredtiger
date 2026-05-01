# test_search_near02 — search_near with key past end of table with timestamps

**File:** `test/suite/test_search_near02.py`
**Storage mode:** General
**Components under test:** cursor search_near, implicit record cursor insert, timestamps, MVCC

## Test Cases

### `test_search_near02.test_implicit_record_cursor_insert_next`
- **What it tests:** Same as `test_search_near01` but with timestamp-based MVCC. The last key is updated or deleted at ts=10. Reads are performed at read_timestamp=5 (before the update/delete). `search_near` past the original last key at ts=5 should find the original value, not the updated one; or correctly return `WT_NOTFOUND` if the key did not exist at ts=5.
- **Components:** `src/cursor/cur_std.c`, `src/txn/`, `src/btree/`
- **Notes:** Parametrized on key format (recno column store / integer row store) and operation type (update vs. delete). Tests that `search_near` respects MVCC timestamps when searching near the table boundary.
