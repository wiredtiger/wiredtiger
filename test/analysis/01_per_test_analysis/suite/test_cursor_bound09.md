# test_cursor_bound09 — Cursor bound with prepared transactions (prepare conflict, ignore_prepare)

**File:** `test/suite/test_cursor_bound09.py`
**Storage mode:** General
**Components under test:** cursor bound API, prepared transactions, WT_PREPARE_CONFLICT, ignore_prepare

## Test Cases

### `test_cursor_bound09.test_cursor_bound_prepared`
- **What it tests:** Tests prepare conflict handling with bounded cursors. Scenarios include: (1) prepared key inside the bounded range — `search()`, `search_near()`, `next()`, `prev()` all return `WT_PREPARE_CONFLICT`; (2) prepared key at the lower bound — `next()` returns prepare conflict; (3) prepared key at the upper bound — `prev()` returns prepare conflict; (4) `ignore_prepare=true` session setting bypasses prepare conflict and treats the key as invisible.
- **Components:** `src/cursor/cur_bound.c`, `src/txn/txn_prepare.c`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: file/table/colgroup × 4 key formats × 7 inclusive combos × ignore_prepare/no_ignore_prepare. Tests that bounds do not mask prepare conflicts for keys within range.
