# test_prepare_cursor01 — Cursor navigation (next/prev) across prepared transaction boundaries

**File:** `test/suite/test_prepare_cursor01.py`
**Storage mode:** General (row-store only)
**Components under test:** prepared transactions, cursor navigation, prepare conflict, isolation levels, timestamps

## Test Cases

### `test_prepare_cursor01.test_cursor_navigate_prepare_transaction`
- **What it tests:** Tests cursor next/prev navigation from multiple timestamp perspectives around a prepared transaction; covers 4 scenarios: (1) prepared insert at a boundary key, (2) prepared update at a boundary key, (3) prepared remove at a boundary key, (4) prepared remove at a non-boundary key; verifies prepare conflict when reading at timestamps after prepare_ts but before commit_ts with `ignore_prepare=false`; verifies correct value before prepare or after commit
- **Components:** `txn/txn_prepare.c`, `cursor/cur_std.c`, `btree/bt_cursor.c`
- **Notes:** Scenarios: read-committed/snapshot isolation levels; column-store explicitly excluded (boundary semantics differ); reads from a second session at various timestamps: before prepare_ts (should succeed with pre-prepare value), between prepare_ts and commit_ts (prepare conflict), after commit_ts (should succeed with committed value); tests both forward (next) and backward (prev) cursor navigation
