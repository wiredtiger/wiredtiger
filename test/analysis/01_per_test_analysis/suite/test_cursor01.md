# test_cursor01 — Cursor forward and backward iteration with duplication

**File:** `test/suite/test_cursor01.py`
**Storage mode:** General
**Components under test:** cursor iteration, duplicate cursors, row-store, column-store

## Test Cases

### `test_cursor01.test_forward_iter`
- **What it tests:** Forward iteration through a populated table using `cursor.next()`, verifying key/value ordering. Exercises both row-store and column-store with file and table URIs.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: file-col (`key_format=r`), file-row (`key_format=S`), table-col, table-row. Duplicate cursor creation is skipped for disagg (layered tables do not support duplicate cursors).

### `test_cursor01.test_backward_iter`
- **What it tests:** Backward iteration through a populated table using `cursor.prev()`, verifying key/value ordering.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`
- **Notes:** Same scenarios as forward iter. Duplicate cursor is skipped for disagg.
