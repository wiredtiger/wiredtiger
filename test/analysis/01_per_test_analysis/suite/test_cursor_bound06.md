# test_cursor_bound06 — Cursor bound with search(): inside, outside, at boundary (inclusive/exclusive)

**File:** `test/suite/test_cursor_bound06.py`
**Storage mode:** General
**Components under test:** cursor bound API, cursor search with bounds, inclusive/exclusive bounds

## Test Cases

### `test_cursor_bound06.test_bound_search_scenario`
- **What it tests:** Exercises `cursor.search()` with bounds set: (1) key inside the bounded range succeeds; (2) key outside the range returns `WT_NOTFOUND`; (3) key exactly at the lower bound returns success for inclusive bound and `WT_NOTFOUND` for exclusive bound; (4) key exactly at the upper bound returns success for inclusive and `WT_NOTFOUND` for exclusive. Tests all combinations across all supported key formats.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: file/table/colgroup × 7 key formats (S, r, i, u, SSS, iS, iSru) × 2 value formats × inclusive × evict/no-evict.
