# test_cursor_bound15 — Bounded search_near exact return value (0/-1/+1) with prefix bounds

**File:** `test/suite/test_cursor_bound15.py`
**Storage mode:** General
**Components under test:** cursor bound API, search_near return value, prefix bounds, eviction

## Test Cases

### `test_cursor_bound15.test_cursor_bound`
- **What it tests:** Verifies the exact return value of `cursor.search_near()` with various bound configurations and the `set_prefix_bound` helper: returns `0` for exact match within bounds, `-1` when the search key is greater than all visible keys in bounds (positioned on smaller key), `+1` when search key is smaller than all visible keys in bounds (positioned on larger key), and `WT_NOTFOUND` when no visible keys exist within bounds.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`
- **Notes:** Scenarios: var_string (key_format=S) and byte_array (key_format=u) × eviction/no-eviction. Tests that the ±1 direction semantics of `search_near()` are preserved when bounds are active.
