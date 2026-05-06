# test_cursor_bound03 — Cursor bound next/prev traversal with lower-only, upper-only, both bounds

**File:** `test/suite/test_cursor_bound03.py`
**Storage mode:** General
**Components under test:** cursor bound API, cursor next, cursor prev, bound traversal

## Test Cases

### `test_cursor_bound03.test_bound_general_scenario`
- **What it tests:** Exercises `cursor.next()` and `cursor.prev()` traversal with: lower-only bound, upper-only bound, both bounds set; out-of-range bounds (bound outside data range); changing bounds mid-traversal; clearing bounds and resuming full traversal. Verifies that only keys within the bound range are visited.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: file/table/colgroup × 7 key formats (S, r, i, u, SSS, iS, iSru) × 2 value formats × 8 inclusive combinations × prev/next. Tests both inclusive and exclusive endpoints at lower and upper bounds.
