# test_txn05 — Commits and rollbacks for truncate operations with logging and recovery

**File:** `test/suite/test_txn05.py`
**Storage mode:** General
**Components under test:** truncate commit/rollback, isolation levels, log recovery, backup

## Test Cases

### `test_txn05.test_ops`
- **What it tests:** Sets up a table with keys {1-5}; performs a single truncate operation (all, both start+stop, start-only, or stop-only); verifies isolation: own session sees current state, snapshot/read-committed see committed, read-uncommitted sees current, backup/recovery sees committed; commits or rolls back; verifies state again; periodically runs `check_log` with log removal enabled/disabled and `wt printlog`.
- **Components:** `txn.c`, `log.c`, `cursor.c`, `backup.c`, `recovery.c`
- **Notes:** Parameterized over row/var × truncate-all/both/start/stop × commit/rollback (16 scenarios). Cycles through 4 sync modes. Specifically tests truncate operations (not DML) with respect to isolation and log-based recovery.
