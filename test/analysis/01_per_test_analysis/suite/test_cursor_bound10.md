# test_cursor_bound10 — Cursor bound with history store: multi-timestamp read scenarios

**File:** `test/suite/test_cursor_bound10.py`
**Storage mode:** General
**Components under test:** cursor bound API, history store, MVCC timestamps, bounded traversal at different read timestamps

## Test Cases

### `test_cursor_bound10.test_bound_general_scenario`
- **What it tests:** Populates three batches of keys at timestamps 50, 200, and 100 respectively, then reads with bounds at read timestamps 10, 75, 150, and 250. Verifies that only keys visible at the given read timestamp and within the bound range are returned. Exercises history store lookup with bounds when older versions reside in the history store (WiredTigerHS.wt).
- **Components:** `src/cursor/cur_bound.c`, `src/history/hs_cursor.c`, `src/txn/txn_timestamp.c`
- **Notes:** Scenarios: file/table/colgroup × 4 key formats × evict × prev/next. Eviction forces older versions to history store. Read at ts=10 sees nothing; ts=75 sees first batch only; ts=150 sees first and third batches; ts=250 sees all.
