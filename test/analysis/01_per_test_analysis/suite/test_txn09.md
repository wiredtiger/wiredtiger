# test_txn09 — Recovery when toggling logging enabled/disabled between operations

**File:** `test/suite/test_txn09.py`
**Storage mode:** General
**Components under test:** logging toggle, recovery, isolation levels

## Test Cases

### `test_txn09.test_ops`
- **What it tests:** Performs 4 sequential insert/remove/update operations on a table, toggling `log=(enabled)` between each operation via `reopen_conn`; after each operation verifies isolation: own session sees current state, snapshot/read-committed see committed, read-uncommitted sees current; commits or rolls back.
- **Components:** `txn.c`, `log.c`, `recovery.c`, `cursor.c`
- **Notes:** Parameterized over row/var × op1-op4 × txn1-txn4 × commit/rollback (pruned to ~20 default / 5000 long). Based on test_txn02 but specifically toggles logging on/off between each operation to test recovery behavior when logging state changes mid-sequence.
