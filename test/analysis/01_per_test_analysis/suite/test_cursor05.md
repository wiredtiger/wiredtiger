# test_cursor05 — Cursor endpoint and reset behavior with column groups

**File:** `test/suite/test_cursor05.py`
**Storage mode:** General
**Components under test:** cursor reset, cursor endpoints, column groups, row-store, column-store

## Test Cases

### `test_cursor05.test_cursor`
- **What it tests:** Cursor state at initialization (no key/value set), endpoints (first/last), and forward/backward iteration with 0, 2, or 4 column groups on empty and non-empty tables. Tests three reset modes: `testmode=0` (cursor.reset()), `testmode=1` (cursor.close()/reopen), `testmode=2` (session.close()/reopen).
- **Components:** `src/cursor/cur_std.c`, `src/schema/`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: row/col × empty/nonempty × no_colgroups/two_colgroups/four_colgroups. Verifies that `get_key()`/`get_value()` raises error on unpositioned cursor.
