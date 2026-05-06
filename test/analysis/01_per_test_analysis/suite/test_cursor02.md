# test_cursor02 — Cursor insert/remove operations with TestCursorTracker verification

**File:** `test/suite/test_cursor02.py`
**Storage mode:** General
**Components under test:** cursor insert, cursor remove, row-store, column-store, cursor position tracking

## Test Cases

### `test_cursor02.test_multiple_remove`
- **What it tests:** Removes multiple keys from a small table and verifies cursor position and data consistency using the TestCursorTracker framework.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_delete.c`
- **Notes:** Scenarios: row and col. TestCursorTracker (`test_cursor_tracker.py`) maintains a shadow data structure for verification.

### `test_cursor02.test_insert_and_remove`
- **What it tests:** Interleaved insert and remove operations, verifying cursor position state after each.
- **Components:** `src/cursor/cur_std.c`, `src/btree/`
- **Notes:** Uses TestCursorTracker for cross-validation.

### `test_cursor02.test_iterate_empty`
- **What it tests:** Iterating over an empty table returns `WT_NOTFOUND`.
- **Components:** `src/cursor/cur_std.c`

### `test_cursor02.test_iterate_one_preexisting`
- **What it tests:** Iterating with exactly one pre-existing key; forward and backward iteration.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`

### `test_cursor02.test_iterate_one_added`
- **What it tests:** Inserting one key then iterating; verifies it is found correctly.
- **Components:** `src/cursor/cur_std.c`
