# test_txn30 — Schema operation failures do not block transaction commit

**File:** `test/suite/test_txn30.py`
**Storage mode:** General
**Components under test:** schema operations in transactions, error state, transaction commit after schema failure

## Test Cases

### `test_txn30.test_txn30`
- **What it tests:** Creates a file with `exclusive=true`; begins a transaction; inserts a value; attempts to create the same file again with `exclusive=true` (which fails because the file already exists); verifies `commit_transaction` still succeeds. Tests that a failed schema operation (create with exclusive flag on existing file) does not set the transaction error flag and block subsequent commit.
- **Components:** `txn.c`, `schema.c`, `session.c`
- **Notes:** No parameterization. Regression test ensuring schema operation failures are isolated from transaction state.
