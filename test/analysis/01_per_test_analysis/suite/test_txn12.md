# test_txn12 — Commit succeeds following a failed open_cursor in a read-only transaction

**File:** `test/suite/test_txn12.py`
**Storage mode:** General
**Components under test:** transaction error state, `open_cursor` failure, commit after error

## Test Cases

### `test_txn12.test_txn12`
- **What it tests:** (1) Begins a read-only transaction, calls `cursor.next()` (fails because table is empty, which is not an error), then calls `open_cursor` with invalid config `next_random=bar` which raises `WiredTigerError`; verifies that `commit_transaction()` still succeeds (open_cursor failure must not set the transaction error flag). (2) Begins a read-write transaction, inserts a record, calls `open_cursor` with invalid config; verifies commit still succeeds.
- **Components:** `txn.c`, `cursor.c`, `session.c`
- **Notes:** No parameterization. Regression test ensuring that a failed `open_cursor` call does not permanently set the transaction error flag, which would prevent commit.
