# test_cursor_bound04 — Cursor bound on positioned cursor, next+prev combination traversal

**File:** `test/suite/test_cursor_bound04.py`
**Storage mode:** General
**Components under test:** cursor bound API, cursor positioning, eviction interaction

## Test Cases

### `test_cursor_bound04.test_bound_special_scenario`
- **What it tests:** Verifies that setting a bound while the cursor is positioned (after a search/next) returns an error (`EINVAL`). Also tests that clearing bounds while positioned works correctly.
- **Components:** `src/cursor/cur_bound.c`
- **Notes:** Scenarios: file/table/colgroup × 7 key formats × 2 value formats × evict/no-evict.

### `test_cursor_bound04.test_bound_combination_scenario`
- **What it tests:** Combines `next()` and `prev()` traversal within a bounded range; verifies that direction reversal within bounds works correctly without returning out-of-bound keys.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`
- **Notes:** Same scenario matrix. Evict scenario forces page eviction mid-traversal to exercise eviction + bounds interaction.
