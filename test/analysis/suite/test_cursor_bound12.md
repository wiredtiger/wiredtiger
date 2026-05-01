# test_cursor_bound12 — search_near visibility with prefix bounds at different read timestamps

**File:** `test/suite/test_cursor_bound12.py`
**Storage mode:** General
**Components under test:** cursor bound API, prefix bounds, search_near, MVCC timestamps, visibility

## Test Cases

### `test_cursor_bound12.test_cursor_bound`
- **What it tests:** Inserts keys with timestamps (some at ts=100, all at ts=250) and calls `search_near()` with prefix bounds at three read timestamps: ts=100 (partial visibility: only some keys visible), ts=25 (nothing visible — all inserts at ts=100+), ts=250 (all keys visible). Verifies that `search_near()` returns `WT_NOTFOUND` when no visible keys exist in the prefix range, and the correct key when they do. Uses `set_prefix_bound` helper.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`, `src/txn/txn_timestamp.c`
- **Notes:** Scenarios: 3 key formats (10s, S, u) × eviction/no-eviction. Key size is 200 bytes for the 10s format. Tests the interaction between prefix bounds and timestamp-based visibility filtering.
