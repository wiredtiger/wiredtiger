# test_cursor_bound19 — Index cursor bounds: traversal, search_near, search, reset, exclusive

**File:** `test/suite/test_cursor_bound19.py`
**Storage mode:** General
**Components under test:** cursor bound API, index cursor, search_near with bounds, index traversal

## Test Cases

### `test_cursor_bound19.test_cursor_index_bounds`
- **What it tests:** Tests cursor bounds on index cursors: forward and backward traversal within bounds, `search_near()` with bounds set, `search()` with bounds, `cursor.reset()` clears bounds, `cursor.bound("action=clear")`, and exclusive lower bound (no keys returned for exclusive lower bound at the minimum key). Verifies that index cursor bounds interact correctly with the secondary index ordering.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_index.c`
- **Notes:** `use_index=True`. Scenarios: table/colgroup × 7 key formats × 6 value formats × evict/no-evict. Index URIs use `"index:<name>:i0"`.
