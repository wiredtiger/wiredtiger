# test_durable_ts02 — Durable timestamp constraint validation (stub/commented-out test)

**File:** `test/suite/test_durable_ts02.py`
**Storage mode:** General
**Components under test:** durable timestamp, prepared transactions, timestamp validation

## Test Cases

### `test_durable_ts03.test_durable_ts03`
- **What it tests:** (Note: the class in this file is named `test_durable_ts03` despite the file being `test_durable_ts02.py`.) The active test body is empty — the entire substantive test logic is commented out with the note `"the system panics if we fail after preparing a transaction"`. The commented-out scenarios were intended to test:
  - Scenario 1: Error when `commit_timestamp > durable_timestamp` (durable must be >= commit).
  - Scenario 2: Error when `durable_timestamp < stable_timestamp` (durable must be >= stable).
  The test class is parameterized for `file`/`table-simple` x `row-string`/`row-int` x three isolation types, but produces no assertions.
- **Components:** `src/txn/txn.c` (timestamp validation)
- **Notes:** This test is a placeholder for future work once the panic-on-prepare-failure issue is resolved. Scenarios: same as test_durable_ts01 (column/recno excluded). The commented-out code expected `WiredTigerError` with messages `/is less than the commit timestamp/` and `/is less than the stable timestamp/`.
