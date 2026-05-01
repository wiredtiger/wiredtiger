# test_cursor_bound14 — Cursor bound with data modification operations (insert, update, reserve, modify, remove)

**File:** `test/suite/test_cursor_bound14.py`
**Storage mode:** General
**Components under test:** cursor bound API, cursor insert/update/reserve/modify/remove with bounds

## Test Cases

### `test_cursor_bound14.test_bound_data_operations`
- **What it tests:** Attempts `insert()`, `update()`, `reserve()`, `modify()`, and `remove()` with bounds set. Keys outside the bound range return `WT_NOTFOUND` or `EINVAL`. Tests boundary inclusivity for update: a key exactly at an exclusive lower bound returns `WT_NOTFOUND`. Tests with both `overwrite=true` and `overwrite=false` cursor configs.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: file/table/colgroup × 7 key formats × 2 value formats × 6 bound configs (lower-only inclusive, lower-only exclusive, upper-only inclusive, upper-only exclusive, both inclusive, both exclusive) × overwrite/no-overwrite.
