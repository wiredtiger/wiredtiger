# test_txn04 — Transactions: hot backup and recovery with truncate operations

**File:** `test/suite/test_txn04.py`
**Storage mode:** General
**Components under test:** transaction commit/rollback, hot backup, recovery, truncate via `wt backup`

## Test Cases

### `test_txn04.test_ops`
- **What it tests:** Sets up a table with keys {1-5}; performs one of: insert, update, remove, or truncate (stop=2, deletes keys 1-2); does a full hot backup before each operation; performs the operation and checks isolation levels; commits or rolls back; does a targeted hot backup of just the URI; verifies the backup shows the committed results after recovery. Cycles through 4 sync modes.
- **Components:** `txn.c`, `log.c`, `backup.c`, `recovery.c`, `cursor.c`
- **Notes:** Parameterized over row/var × insert/update/remove/truncate-stop × commit/rollback (16 scenarios). Uses `wt backup` command-line tool. Tests that hot backups combined with recovery correctly reflect committed state including for truncate operations.
