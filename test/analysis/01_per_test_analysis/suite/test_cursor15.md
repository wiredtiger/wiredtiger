# test_cursor15 — read_once cursor config and WT_READ_WONT_NEED eviction path

**File:** `test/suite/test_cursor15.py`
**Storage mode:** General
**Components under test:** cursor read_once config, eviction (WT_READ_WONT_NEED), cursor caching interaction

## Test Cases

### `test_cursor15.test_cursor15`
- **What it tests:** Opens a cursor with `read_once=true` and scans 20 documents of 100KB each with only 1MB cache. Verifies that the `WT_READ_WONT_NEED` eviction hint is exercised (pages are evicted promptly after reads). Also verifies that `read_once=true` works correctly when cursor caching (`cache_cursors=true`) is also enabled.
- **Components:** `src/cursor/cur_std.c`, `src/evict/`, `src/btree/bt_read.c`
- **Notes:** Cache is intentionally undersized (1MB) relative to data (20 × 100KB = 2MB) to force eviction pressure. Tests that no error occurs and all values are read correctly.
