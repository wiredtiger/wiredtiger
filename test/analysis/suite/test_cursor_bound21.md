# test_cursor_bound21 — Cursor bound with prepare conflicts (lower/upper bound on prepared keys, non-inclusive)

**File:** `test/suite/test_cursor_bound21.py`
**Storage mode:** General
**Components under test:** cursor bound API, prepared transactions, WT_PREPARE_CONFLICT, non-inclusive bounds

## Test Cases

### `test_cursor_bound21.test_cursor_bound_bug`
- **What it tests:** Sets a lower bound exactly on a prepared (uncommitted) key. Calls `cursor.next()` starting from before the prepared key; expects `WT_PREPARE_CONFLICT` to be returned three consecutive times (validating that the conflict is re-raised on retry). Tests that bounds do not suppress prepare conflicts for in-range prepared keys.
- **Components:** `src/cursor/cur_bound.c`, `src/txn/txn_prepare.c`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: 4 key formats (S, r, i, u).

### `test_cursor_bound21.test_not_inclusive_bound`
- **What it tests:** Sets a non-inclusive lower bound on a prepared key and calls `next()`; verifies that with exclusive lower bound, the prepared key is outside the range and `WT_PREPARE_CONFLICT` is not raised (cursor skips past it). Also tests upper bound with `prev()` and a prepared key at the exclusive upper bound.
- **Components:** `src/cursor/cur_bound.c`, `src/txn/txn_prepare.c`

### `test_cursor_bound21.test_missing_bound_key_prepare`
- **What it tests:** Sets a bound to a key that does not exist in the data (not a prepared key), then inserts a prepared value at a different key within the range. Verifies that the bound is correctly established even when the bound key itself has no data.
- **Components:** `src/cursor/cur_bound.c`, `src/txn/txn_prepare.c`
