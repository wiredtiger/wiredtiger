# test_cursor03 — Cursor operations on larger tables with variable key/value sizes

**File:** `test/suite/test_cursor03.py`
**Storage mode:** General
**Components under test:** cursor insert, cursor remove, row-store, column-store, large key/value payloads

## Test Cases

### `test_cursor03.test_multiple_remove`
- **What it tests:** Removes many keys from tables of 1000 or 10000 entries using TestCursorTracker, with optional variable key and value sizes (None or [10, 10000] bytes).
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_delete.c`
- **Notes:** Scenarios: row/col × key/val sizes (None or [10,10000]) × tablecount (1000/10000). Large key/value sizes exercise overflow items.

### `test_cursor03.test_insert_and_remove`
- **What it tests:** Interleaved insert/remove on larger tables with variable key/value sizes. Uses TestCursorTracker for cross-validation.
- **Components:** `src/cursor/cur_std.c`, `src/btree/`
- **Notes:** Same scenario matrix as test_multiple_remove.
