# test_cursor_bound13 — Bounded search_near across multiple pages with large keys (WT-7912)

**File:** `test/suite/test_cursor_bound13.py`
**Storage mode:** General
**Components under test:** cursor bound API, search_near, multi-page traversal, large keys

## Test Cases

### `test_cursor_bound13.test_search_near`
- **What it tests:** Inserts large keys (key_size=200 bytes) across multiple pages, sets bounds, and calls `search_near()` for a key that lies on a different page from the starting position. Verifies that bounded `search_near()` correctly finds the nearest visible key even when it must traverse page boundaries. Regression test for WT-7912 where bounded search_near failed to find a key on a different page.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: var_string (key_format=S) and byte_array (key_format=u). Large key size (200 bytes) forces multiple pages.
