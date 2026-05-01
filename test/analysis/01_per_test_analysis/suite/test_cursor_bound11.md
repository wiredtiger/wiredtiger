# test_cursor_bound11 — Prefix bound with search_near: unique index simulation, skip count stats, prepare

**File:** `test/suite/test_cursor_bound11.py`
**Storage mode:** General
**Components under test:** cursor bound API, prefix bounds (set_prefix_bound), search_near, unique index simulation, statistics

## Test Cases

### `test_cursor_bound11.test_base_scenario`
- **What it tests:** Sets a prefix bound using the `set_prefix_bound` helper and calls `search_near()` with various keys. Verifies that only keys within the prefix are returned and that search_near correctly returns the nearest key within bounds.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`
- **Notes:** `conn_config='statistics=(all)'`.

### `test_cursor_bound11.test_unique_index_case`
- **What it tests:** Simulates a unique index use case with prefix bounds: searches for a key prefix to check uniqueness. Verifies that `search_near()` with prefix bounds efficiently determines whether a prefixed key exists.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`

### `test_cursor_bound11.test_row_search`
- **What it tests:** Verifies that the number of keys examined by `search_near()` is reduced (skip count decreases) when prefix bounds restrict the search range, versus unbounded search_near.
- **Components:** `src/cursor/cur_bound.c`, `src/stat/`

### `test_cursor_bound11.test_prepared`
- **What it tests:** With a prepared key near the prefix bound, verifies that `search_near()` handles `WT_PREPARE_CONFLICT` correctly within the prefix-bounded range.
- **Components:** `src/cursor/cur_bound.c`, `src/txn/txn_prepare.c`
