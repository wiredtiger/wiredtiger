# test_cursor04 — Cursor search() and search_near() exact match and boundary behavior

**File:** `test/suite/test_cursor04.py`
**Storage mode:** General
**Components under test:** cursor search, cursor search_near, row-store, column-store

## Test Cases

### `test_cursor04.test_searches`
- **What it tests:** Exercises `cursor.search()` for exact match, key beyond the end of the table (expects `WT_NOTFOUND`), and `cursor.search_near()` for deleted keys (returns nearest neighbor with direction indicator) and keys past end. Verifies return codes and values.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: row (`key_format=S`) and col (`key_format=r`). Tests search after deleting middle keys; search_near must return the closest existing key.
