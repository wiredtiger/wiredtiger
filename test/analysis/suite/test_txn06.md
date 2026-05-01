# test_txn06 — Long-running snapshots: verbose transaction logging of pinned snapshots

**File:** `test/suite/test_txn06.py`
**Storage mode:** General
**Components under test:** snapshot pinning, long-running transactions, `verbose=[transaction]`, "pinned in session" message

## Test Cases

### `test_txn06.test_long_running`
- **What it tests:** Populates a source table with 100,000 rows; then scans it in the self.session (which keeps a snapshot pinned) while inserting each row into a new table in a separate session (which allocates new transaction IDs); verifies that a "pinned in session" verbose message appears, confirming the transaction subsystem detects and logs long-running snapshot pins.
- **Components:** `txn.c`, `verbose.c`
- **Notes:** Parameterized over row (string key) and var (record number key). Uses `verbose=[transaction]` connection config. Tests the diagnostic path that emits warnings about sessions with old pinned snapshots blocking transaction ID recycling.
