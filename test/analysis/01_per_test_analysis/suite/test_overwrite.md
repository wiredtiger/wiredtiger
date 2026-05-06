# test_overwrite — Cursor overwrite=false behavior for insert, remove, and update

**File:** `test/suite/test_overwrite.py`
**Storage mode:** General
**Components under test:** cursor API, overwrite configuration

## Test Cases

### `test_overwrite.test_overwrite_insert`
- **What it tests:** Cursor with `overwrite=false` returns `WT_DUPLICATE_KEY` when inserting a key that already exists; cursor with `overwrite=true` (duplicated from the overwrite=false cursor) can insert over an existing key
- **Components:** `cursor/cur_std.c`, `btree/bt_cursor.c`
- **Notes:** Scenarios cover file/table × column/integer-row key formats × 5 whitespace/padding variants of the `overwrite=false` config string (e.g., `'overwrite=false'`, `' overwrite=false'`, `'overwrite = false'`, etc.)

### `test_overwrite.test_overwrite_remove`
- **What it tests:** `cursor.remove()` is not affected by the overwrite setting; removing an existing key always succeeds regardless of whether overwrite=false or overwrite=true
- **Components:** `cursor/cur_std.c`, `btree/bt_cursor.c`
- **Notes:** Same scenario matrix as test_overwrite_insert

### `test_overwrite.test_overwrite_update`
- **What it tests:** Cursor with `overwrite=false` returns `WT_NOTFOUND` when updating a key that does not exist; confirms update on existing key still succeeds with overwrite=false
- **Components:** `cursor/cur_std.c`, `btree/bt_cursor.c`
- **Notes:** Same scenario matrix as test_overwrite_insert
