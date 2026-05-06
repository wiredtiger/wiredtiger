# test_cursor11 — Cursor position state after remove and insert

**File:** `test/suite/test_cursor11.py`
**Storage mode:** General
**Components under test:** cursor remove, cursor insert, cursor position, row-store, column-store, index

## Test Cases

### `test_cursor11.test_cursor_remove_with_position`
- **What it tests:** Removes a key while the cursor is positioned on it; verifies the cursor no longer has a valid position after removal.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_delete.c`
- **Notes:** Scenarios: file/table-complex/table-index/table-simple × integer/recno/string keys.

### `test_cursor11.test_cursor_remove_without_position`
- **What it tests:** Sets a key and removes without first positioning (no prior search/next); verifies behavior when cursor had no prior position.
- **Components:** `src/cursor/cur_std.c`

### `test_cursor11.test_cursor_remove_with_key_and_position`
- **What it tests:** Positions the cursor via search then sets a different key and removes; verifies which key is removed.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_delete.c`

### `test_cursor11.test_cursor_insert`
- **What it tests:** Verifies cursor state after insert: key is unset, and that next/prev still work to position the cursor.
- **Components:** `src/cursor/cur_std.c`
