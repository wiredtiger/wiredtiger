# test_txn02 — Commits and rollbacks with logging, sync modes, and backup recovery

**File:** `test/suite/test_txn02.py`
**Storage mode:** General (skipped for tiered)
**Components under test:** transaction commit/rollback, isolation levels, `log_flush`, backup recovery, log removal

## Test Cases

### `test_txn02.test_ops`
- **What it tests:** Sets up a table with keys {1, 2, 10, 11}; performs 4 sequential operations (insert, remove, or update) each followed by either commit or rollback; after each operation and after each commit/rollback verifies that: (1) own session sees current state; (2) snapshot and read-committed see only committed state; (3) read-uncommitted sees current state; (4) a backup opened with recovery sees committed state. Periodically runs `check_log` which verifies log file removal behavior and `wt printlog` exits cleanly.
- **Components:** `txn.c`, `log.c`, `cursor.c`, `backup.c`, `recovery.c`
- **Notes:** Parameterized over row/var × op1-op4 × txn1-txn4 × commit/rollback combinations (pruned to ~20 default / 5000 long). Cycles through 4 sync modes (dsync, fsync, none, disabled) and optionally zerofill. Tests multi-operation transaction state across all isolation levels, reopen, and backup recovery.
