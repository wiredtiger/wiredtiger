# test_txn18 — Recovery settings: recover=error vs recover=on

**File:** `test/suite/test_txn18.py`
**Storage mode:** General
**Components under test:** `log=(recover=error)`, `log=(recover=on)`, crash recovery

## Test Cases

### `test_txn18.test_recovery`
- **What it tests:** Creates a table, checkpoints, inserts 10,000 records; copies to ERROR and RESTART directories; closes the connection; verifies that opening either directory with `log=(recover=error)` raises "recovery must be run" error; opens RESTART with `log=(recover=on)` and verifies all 10,000 records are present; closes and reopens RESTART with `log=(recover=error)` confirming that after a clean shutdown, `recover=error` succeeds.
- **Components:** `log.c`, `recovery.c`
- **Notes:** Parameterized over integer-row and column formats. Tests the explicit recovery mode settings: `recover=error` fails if recovery is needed, `recover=on` forces recovery, and `recover=error` succeeds after a clean shutdown.
