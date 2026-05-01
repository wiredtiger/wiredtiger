# test_cursor_bound_fuzz — Randomized cursor bound fuzzer with upsert/remove/truncate/prepare

**File:** `test/suite/test_cursor_bound_fuzz.py`
**Storage mode:** General
**Components under test:** cursor bound API, cursor next/prev/search/search_near, random operations, prepared transactions

## Test Cases

### `test_cursor_bound_fuzz.test_bound_fuzz`
- **What it tests:** Randomized fuzzer running 50 iterations (200 for long tests) with random key ranges (1–1000, or 1–10000 for long tests). Each iteration: picks random lower/upper bounds with random inclusive/exclusive flags, performs random upsert/remove/truncate operations on the data, with 5% probability of wrapping operations in a prepared transaction. Then calls `next()`, `prev()`, `search()`, and `search_near()` and validates results against an in-memory Python shadow state. Catches any discrepancy between WiredTiger output and expected behavior.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`, `src/btree/bt_delete.c`, `src/txn/`
- **Notes:** Scenarios: file/table × row (`key_format=i`) / column (`key_format=r`). Shadow state tracks current data set as a sorted dict. Fuzzer seeds are printed for reproducibility on failure.
