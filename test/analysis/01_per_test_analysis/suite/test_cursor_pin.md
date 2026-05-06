# test_cursor_pin — Fast-path pinned-page search (sequential and cross-page)

**File:** `test/suite/test_cursor_pin.py`
**Storage mode:** General
**Components under test:** cursor search fast path, pinned page optimization, row-store, column-store

## Test Cases

### `test_cursor_pin.test_smoke`
- **What it tests:** Basic pinned-page search: two consecutive searches for the same key — second search hits the fast path (page is already pinned).
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`
- **Notes:** File URI. Scenarios: recno (`key_format=r`) and string (`key_format=S`).

### `test_cursor_pin.test_basic`
- **What it tests:** Sequential search across keys that span multiple pages. Verifies that pinned-page fast path is used for adjacent keys on the same page, and that cross-page searches also succeed correctly. Tests both forward sequential search and backward sequential search.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`

### `test_cursor_pin.test_missing`
- **What it tests:** Column-store with a gap (missing key) in the middle of the key range. Searches around the gap with pinned-page optimization active; verifies that missing keys return `WT_NOTFOUND` and that the search continues correctly after the gap.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`, `src/cursor/cur_col.c`
- **Notes:** Column-store only (`key_format=r`).
