# test_cursor20 — WT_DUPLICATE_KEY return and get_value() after duplicate insert

**File:** `test/suite/test_cursor20.py`
**Storage mode:** General
**Components under test:** cursor insert, WT_DUPLICATE_KEY, cursor value state after error

## Test Cases

### `test_cursor20.test_dup_key`
- **What it tests:** Inserts a key-value pair, then inserts the same key again with a different value using `overwrite=false`. Expects `WT_DUPLICATE_KEY`. After the error, verifies that `cursor.get_value()` returns the existing (pre-existing) value for the duplicate key, not the new value.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: row/var × in-memory/on-disk. Tests that the cursor value is correctly set to the existing record's value on `WT_DUPLICATE_KEY`, which allows the caller to inspect what value already existed.
