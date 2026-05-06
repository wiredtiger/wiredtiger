# test_cursor_bound08 — Cursor bound statistics (early exit, unpositioned, repositioned, skip count)

**File:** `test/suite/test_cursor_bound08.py`
**Storage mode:** General
**Components under test:** cursor bound API, cursor bound statistics, eviction interaction

## Test Cases

### `test_cursor_bound08.test_bound_basic_stat_scenario`
- **What it tests:** Verifies that the following statistics increment correctly when bounds cause early termination or skipping: `cursor_bounds_next_early_exit`, `cursor_bounds_prev_early_exit`, `cursor_bounds_next_unpositioned`, `cursor_bounds_prev_unpositioned`, `cursor_bounds_reset`, `cursor_bounds_search_early_exit`, `cursor_bounds_search_near_repositioned_cursor`.
- **Components:** `src/cursor/cur_bound.c`, `src/stat/`
- **Notes:** `conn_config='statistics=(all)'`. Scenarios: file/table × 7 key formats × evict.

### `test_cursor_bound08.test_bound_perf_stat_scenario`
- **What it tests:** With invisible data (uncommitted transactions) at the boundary, verifies that the skip count for `search_near` is reduced when bounds are set (fewer keys need to be scanned to determine there are no visible keys in range). Tests that `cursor_bounds_search_near_repositioned_cursor` counts repositioning events.
- **Components:** `src/cursor/cur_bound.c`, `src/txn/`, `src/stat/`
- **Notes:** Same scenario matrix. Uses a separate uncommitted session to make boundary keys invisible to the reading cursor.
