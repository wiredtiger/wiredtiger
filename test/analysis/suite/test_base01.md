# test_base01 — Basic table creation, error handling, and key/value insertion

**File:** `test/suite/test_base01.py`
**Storage mode:** General
**Components under test:** session API (create), cursor API (insert, search), error handling

## Test Cases

### `test_base01.test_error`
- **What it tests:** Attempts to create a table with an invalid configuration string (`expect_this_error,okay?`) and verifies that `WiredTigerError` is raised with message containing `nvalid argument` and that stderr contains `unknown configuration key`.
- **Components:** `src/session/session_api.c`, `src/config/config_api.c`

### `test_base01.test_empty`
- **What it tests:** Creates a table and searches for a nonexistent key, asserting `WT_NOTFOUND`.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_search.c`

### `test_base01.test_insert`
- **What it tests:** Creates a table, inserts a key/value pair via `cursor.insert()`, then reads it back via `cursor.search()` and verifies the value matches.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_walk.c`
- **Notes:** Parametrized across `column` (r key) and `row_string` (S key) formats. Uses small page sizes (allocation_size=512, internal_page_max=16384, leaf_page_max=131072).
